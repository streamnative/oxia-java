/*
 * Copyright © 2022-2024 StreamNative Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.oxia.client.notify;

import static io.streamnative.oxia.proto.NotificationType.KEY_CREATED;
import static io.streamnative.oxia.proto.NotificationType.KEY_DELETED;
import static io.streamnative.oxia.proto.NotificationType.KEY_MODIFIED;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.streamnative.oxia.client.CompositeConsumer;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.api.Notification.KeyModified;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.grpc.OxiaStubManager;
import io.streamnative.oxia.client.metrics.NotificationMetrics;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Added;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Reassigned;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Removed;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class NotificationManagerTest {

    @Nested
    @DisplayName("Simple lifecycle tests")
    class SimpleLifecycleTest {

        @Mock ShardManager shardManager;
        @Mock ShardManager.Assignments assignments;
        @Mock ShardNotificationReceiver.Factory receiverFactory;
        @Mock ShardNotificationReceiver receiver1;
        @Mock ShardNotificationReceiver receiver2;
        @Mock ShardNotificationReceiver receiver3;
        @Mock NotificationMetrics metrics;
        NotificationManager manager;
        CompositeConsumer<Notification> callback = new CompositeConsumer<>();

        @BeforeEach
        void setup() {
            manager = new NotificationManager(receiverFactory, shardManager, callback, metrics);
        }

        @Test
        void startOnRegisterCallback() {
            when(receiverFactory.newReceiver(1L, "leader1", metrics)).thenReturn(receiver1);
            when(receiverFactory.newReceiver(2L, "leader2", metrics)).thenReturn(receiver2);
            when(shardManager.getAll()).thenReturn(List.of(1L, 2L));
            when(shardManager.leader(1L)).thenReturn("leader1");
            when(shardManager.leader(2L)).thenReturn("leader2");
            when(receiver1.start()).thenReturn(CompletableFuture.completedFuture(null));
            when(receiver2.start()).thenReturn(CompletableFuture.completedFuture(null));

            manager.registerCallback(n -> {});

            verify(receiver1).start();
            verify(receiver2).start();
        }

        @Test
        void acceptNotStarted() {
            var changes =
                    new ShardAssignmentChanges(
                            Set.of(new Added(1L, "leader1"), new Added(2L, "leader2")), Set.of(), Set.of());
            manager.accept(changes);

            verifyNoInteractions(receiverFactory, receiver1, receiver2, receiver3);
        }

        @Test
        void acceptRemoveShard() {
            when(receiverFactory.newReceiver(1L, "leader1", metrics)).thenReturn(receiver1);
            when(receiverFactory.newReceiver(2L, "leader2", metrics)).thenReturn(receiver2);
            when(shardManager.getAll()).thenReturn(List.of(1L, 2L));
            when(shardManager.leader(1L)).thenReturn("leader1");
            when(shardManager.leader(2L)).thenReturn("leader2");
            when(receiver1.start()).thenReturn(CompletableFuture.completedFuture(null));
            when(receiver2.start()).thenReturn(CompletableFuture.completedFuture(null));

            manager.registerCallback(n -> {});

            var changes =
                    new ShardAssignmentChanges(Set.of(), Set.of(new Removed(2L, "leader2")), Set.of());
            manager.accept(changes);

            verify(receiver2).close();
        }

        @Test
        void acceptAddShard() {
            when(receiverFactory.newReceiver(1L, "leader1", metrics)).thenReturn(receiver1);
            when(receiverFactory.newReceiver(2L, "leader2", metrics)).thenReturn(receiver2);
            when(shardManager.getAll()).thenReturn(List.of(1L, 2L));
            when(shardManager.leader(1L)).thenReturn("leader1");
            when(shardManager.leader(2L)).thenReturn("leader2");
            when(receiver1.start()).thenReturn(CompletableFuture.completedFuture(null));
            when(receiver2.start()).thenReturn(CompletableFuture.completedFuture(null));

            manager.registerCallback(n -> {});

            when(receiverFactory.newReceiver(3L, "leader3", metrics)).thenReturn(receiver3);
            when(receiver3.start()).thenReturn(CompletableFuture.completedFuture(null));

            var changes =
                    new ShardAssignmentChanges(Set.of(new Added(3L, "leader3")), Set.of(), Set.of());
            manager.accept(changes);

            verify(receiver3).start();
        }

        @Test
        void acceptReassignShard() {
            when(receiverFactory.newReceiver(1L, "leader1", metrics)).thenReturn(receiver1);
            when(receiverFactory.newReceiver(2L, "leader2", metrics)).thenReturn(receiver2);
            when(shardManager.getAll()).thenReturn(List.of(1L, 2L));
            when(shardManager.leader(1L)).thenReturn("leader1");
            when(shardManager.leader(2L)).thenReturn("leader2");
            when(receiver1.start()).thenReturn(CompletableFuture.completedFuture(null));
            when(receiver2.start()).thenReturn(CompletableFuture.completedFuture(null));

            manager.registerCallback(n -> {});

            when(receiverFactory.newReceiver(2L, "leader3", metrics)).thenReturn(receiver3);
            var shard2offset = 1000L;
            when(receiver2.getOffset()).thenReturn(shard2offset);

            var changes =
                    new ShardAssignmentChanges(
                            Set.of(), Set.of(), Set.of(new Reassigned(2L, "leader2", "leader3")));
            manager.accept(changes);

            verify(receiver2).close();
            verify(receiver3).start(Optional.of(shard2offset));
        }

        @Test
        void closeNotStarted() {
            assertThatNoException().isThrownBy(() -> manager.close());
        }

        @Test
        void close() throws Exception {
            when(receiverFactory.newReceiver(1L, "leader1", metrics)).thenReturn(receiver1);
            when(receiverFactory.newReceiver(2L, "leader2", metrics)).thenReturn(receiver2);
            when(shardManager.getAll()).thenReturn(List.of(1L, 2L));
            when(shardManager.leader(1L)).thenReturn("leader1");
            when(shardManager.leader(2L)).thenReturn("leader2");
            when(receiver1.start()).thenReturn(CompletableFuture.completedFuture(null));
            when(receiver2.start()).thenReturn(CompletableFuture.completedFuture(null));

            manager.registerCallback(n -> {});

            manager.close();

            verify(receiver1).close();
            verify(receiver2).close();

            assertThatThrownBy(() -> manager.registerCallback(callback))
                    .isInstanceOf(IllegalStateException.class);

            var changes1 =
                    new ShardAssignmentChanges(
                            Set.of(new Added(1L, "leader1"), new Added(2L, "leader2")), Set.of(), Set.of());
            manager.accept(changes1);

            verifyNoMoreInteractions(receiverFactory, receiver1, receiver2);
        }
    }

    @Nested
    @DisplayName("GRPC tests")
    class GrpcTest {

        BlockingQueue<Flux<NotificationBatch>> responses1 = new ArrayBlockingQueue<>(10);
        BlockingQueue<Flux<NotificationBatch>> responses2 = new ArrayBlockingQueue<>(10);

        ReactorOxiaClientGrpc.OxiaClientImplBase serviceImpl1 =
                new ReactorOxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public Flux<NotificationBatch> getNotifications(Mono<NotificationsRequest> request) {
                        Flux<NotificationBatch> assignments = responses1.poll();
                        if (assignments == null) {
                            return Flux.error(Status.RESOURCE_EXHAUSTED.asException());
                        }
                        return assignments;
                    }
                };

        ReactorOxiaClientGrpc.OxiaClientImplBase serviceImpl2 =
                new ReactorOxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public Flux<NotificationBatch> getNotifications(Mono<NotificationsRequest> request) {
                        Flux<NotificationBatch> assignments = responses2.poll();
                        if (assignments == null) {
                            return Flux.error(Status.RESOURCE_EXHAUSTED.asException());
                        }
                        return assignments;
                    }
                };

        String serverName1 = InProcessServerBuilder.generateName();
        String serverName2 = InProcessServerBuilder.generateName();
        Server server1;
        Server server2;
        ManagedChannel channel1;
        ManagedChannel channel2;

        long shardId1 = 1L;
        long shardId2 = 2L;
        @Mock OxiaStubManager stubManager;
        @Mock ShardManager shardManager;
        @Mock ShardManager.Assignments assignments;
        @Mock Consumer<Notification> notificationCallback;
        @Mock Metrics metrics;
        @Mock Metrics.Histogram histogram;

        @BeforeEach
        void beforeEach() throws Exception {
            responses1.clear();
            responses2.clear();
            server1 =
                    InProcessServerBuilder.forName(serverName1)
                            .directExecutor()
                            .addService(serviceImpl1)
                            .build()
                            .start();
            server2 =
                    InProcessServerBuilder.forName(serverName2)
                            .directExecutor()
                            .addService(serviceImpl2)
                            .build()
                            .start();
            channel1 = InProcessChannelBuilder.forName(serverName1).directExecutor().build();
            channel2 = InProcessChannelBuilder.forName(serverName2).directExecutor().build();
            var stub1 = new OxiaStub(channel1);
            var stub2 = new OxiaStub(channel2);
            when(stubManager.getStub("leader1")).thenReturn(stub1);
            when(stubManager.getStub("leader2")).thenReturn(stub2);
            when(metrics.histogram(anyString(), any(Metrics.Unit.class))).thenReturn(histogram);
        }

        @Test
        void notificationsFromMultipleShards() throws Exception {
            var notifications1 =
                    NotificationBatch.newBuilder()
                            .putNotifications("key1", created(1L))
                            .putNotifications("key3", modified(3L))
                            .build();

            var notifications2 =
                    NotificationBatch.newBuilder().putNotifications("key2", deleted()).build();

            responses1.offer(Flux.just(notifications1).concatWith(Flux.never()));
            responses2.offer(Flux.just(notifications2).concatWith(Flux.never()));

            try (var manager = new NotificationManager(stubManager, shardManager, metrics)) {
                manager.registerCallback(notificationCallback);
                var changes =
                        new ShardAssignmentChanges(
                                Set.of(new Added(1L, "leader1"), new Added(2L, "leader2")), Set.of(), Set.of());
                manager.accept(changes);
                await()
                        .untilAsserted(
                                () -> {
                                    verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                                    verify(notificationCallback).accept(new KeyDeleted("key2"));
                                    verify(notificationCallback).accept(new KeyModified("key3", 3L));
                                });
            }
        }

        @AfterEach
        void afterEach() {
            channel1.shutdownNow();
            channel2.shutdownNow();
            server1.shutdownNow();
            server2.shutdownNow();
        }

        static io.streamnative.oxia.proto.Notification created(long version) {
            return io.streamnative.oxia.proto.Notification.newBuilder()
                    .setType(KEY_CREATED)
                    .setVersionId(version)
                    .build();
        }

        static io.streamnative.oxia.proto.Notification deleted() {
            return io.streamnative.oxia.proto.Notification.newBuilder().setType(KEY_DELETED).build();
        }

        static io.streamnative.oxia.proto.Notification modified(long version) {
            return io.streamnative.oxia.proto.Notification.newBuilder()
                    .setType(KEY_MODIFIED)
                    .setVersionId(version)
                    .build();
        }
    }
}
