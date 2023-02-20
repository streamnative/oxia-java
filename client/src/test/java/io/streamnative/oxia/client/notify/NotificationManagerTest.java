/*
 * Copyright © 2022-2023 StreamNative Inc.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

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
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class NotificationManagerTest {

    @Nested
    @DisplayName("Simple lifecycle tests")
    class SimpleLifecycleTest {

        @Mock ShardManager.Assignments assignments;
        @Mock Function<Long, ShardNotificationReceiver> receiverFactory;
        @Mock ShardNotificationReceiver receiver1;
        @Mock ShardNotificationReceiver receiver2;
        @Mock ShardNotificationReceiver receiver3;
        NotificationManager manager;
        CompositeConsumer<Notification> callback = new CompositeConsumer<>();

        @BeforeEach
        void setup() {
            manager = new NotificationManager(receiverFactory, callback);
            when(receiverFactory.apply(1L)).thenReturn(receiver1);
            when(receiverFactory.apply(2L)).thenReturn(receiver2);
            when(receiver1.start()).thenReturn(CompletableFuture.completedFuture(null));
            when(receiver2.start()).thenReturn(CompletableFuture.completedFuture(null));
            //            when(receiver1.getShardId()).thenReturn(1L);
            //            when(receiver2.getShardId()).thenReturn(2L);
            //            when(receiver1.getLeader()).thenReturn("leader1");
            //            when(receiver2.getLeader()).thenReturn("leader2");

            when(assignments.getAll()).thenReturn(List.of(1L, 2L));
            //            when(assignments.leader(1L)).thenReturn("leader1");
            //            when(assignments.leader(2L)).thenReturn("leader2");
        }

        @Test
        void accept() {
            manager.accept(assignments);

            verify(receiver1).start();
            verify(receiver2).start();
        }

        @Test
        void acceptRemoveShard() {
            manager.accept(assignments);

            verify(receiver1).start();
            verify(receiver2).start();

            when(assignments.getAll()).thenReturn(List.of(1L));
            when(receiver1.getLeader()).thenReturn("leader1");
            when(assignments.leader(1L)).thenReturn("leader1");

            manager.accept(assignments);

            verify(receiver2).close();
        }

        @Test
        void acceptAddShard() {
            manager.accept(assignments);

            verify(receiver1).start();
            verify(receiver2).start();

            when(assignments.getAll()).thenReturn(List.of(1L, 2L, 3L));
            when(receiverFactory.apply(3L)).thenReturn(receiver3);
            when(receiver3.start()).thenReturn(CompletableFuture.completedFuture(null));

            when(receiver1.getLeader()).thenReturn("leader1");
            when(receiver2.getLeader()).thenReturn("leader2");
            when(assignments.leader(1L)).thenReturn("leader1");
            when(assignments.leader(2L)).thenReturn("leader2");

            manager.accept(assignments);

            verify(receiver3).start();
        }

        @Test
        void acceptReassignShard() {
            manager.accept(assignments);

            verify(receiver1).start();
            verify(receiver2).start();

            when(receiver1.getLeader()).thenReturn("leader1");
            when(receiver2.getLeader()).thenReturn("leader2");
            when(assignments.leader(1L)).thenReturn("leader1");
            when(assignments.leader(2L)).thenReturn("leader2_2");

            when(receiverFactory.apply(2L)).thenReturn(receiver3);
            var shard2offset = 1000L;
            when(receiver2.getOffset()).thenReturn(shard2offset);

            manager.accept(assignments);

            verify(receiver2).close();
            verify(receiver3).start(shard2offset);
        }

        @Test
        void close() throws Exception {
            manager.accept(assignments);
            manager.close();
            verify(receiverFactory).apply(1L);
            verify(receiverFactory).apply(2L);

            verify(receiver1).close();
            verify(receiver2).close();

            assertThatThrownBy(() -> manager.registerCallback(callback))
                    .isInstanceOf(IllegalStateException.class);

            manager.accept(assignments);
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
        @Mock Function<Long, ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubByShardId;
        @Mock Function<Long, String> leaderByShardId;
        @Mock ShardManager.Assignments assignments;
        @Mock Consumer<Notification> notificationCallback;

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
            var stub1 = ReactorOxiaClientGrpc.newReactorStub(channel1);
            var stub2 = ReactorOxiaClientGrpc.newReactorStub(channel2);
            when(stubByShardId.apply(shardId1)).thenReturn(stub1);
            when(stubByShardId.apply(shardId2)).thenReturn(stub2);
            when(assignments.getAll()).thenReturn(List.of(shardId1, shardId2));
            when(leaderByShardId.apply(anyLong()))
                    .thenAnswer((Answer<String>) i -> "leader" + i.getArguments()[0].toString());
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

            try (var manager = new NotificationManager(stubByShardId, leaderByShardId)) {
                manager.registerCallback(notificationCallback);
                manager.accept(assignments);
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
