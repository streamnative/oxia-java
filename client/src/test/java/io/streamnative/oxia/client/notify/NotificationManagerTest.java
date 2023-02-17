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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.api.Notification.KeyModified;
import io.streamnative.oxia.client.notify.NotificationManager.CompositeCallback;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import java.time.Duration;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class NotificationManagerTest {

    @Nested
    @DisplayName("Simple lifecycle tests")
    class SimpleLifecycleTest {

        @Mock Function<Long, ShardNotificationReceiver> receiverFactory;
        @Mock ShardManager shardManager;
        @Mock ShardNotificationReceiver receiver1, receiver2;
        @Mock Consumer<Notification> callback1, callback2;
        NotificationManager manager;
        CompositeCallback callback = new CompositeCallback();

        @BeforeEach
        void setup() {
            manager = new NotificationManager(shardManager, receiverFactory, callback);
            when(shardManager.getAll()).thenReturn(List.of(1L, 2L));
            when(receiverFactory.apply(1L)).thenReturn(receiver1);
            when(receiverFactory.apply(2L)).thenReturn(receiver2);
            when(receiver1.start()).thenReturn(CompletableFuture.completedFuture(null));
            when(receiver2.start()).thenReturn(CompletableFuture.completedFuture(null));
            when(receiver1.getShardId()).thenReturn(1L);
            when(receiver2.getShardId()).thenReturn(2L);
        }

        @Test
        void startIfRequired() {
            var started = manager.startIfRequired();
            verify(shardManager).getAll();
            assertThat(started).isCompleted();
            verify(receiver1).start();
            verify(receiver2).start();
            assertThat(manager.startIfRequired()).isSameAs(started);
            verifyNoMoreInteractions(shardManager);
        }

        @Test
        void start() {
            assertThat(manager.start()).isCompleted();
            verify(receiver1).start();
            verify(receiver2).start();
        }

        @Test
        void close() throws Exception {
            manager.start().join();
            manager.close();
            verify(receiver1).close();
            verify(receiver2).close();
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

        String serverAddress1 = "address1";
        String serverAddress2 = "address2";
        String serverName1 = InProcessServerBuilder.generateName();
        String serverName2 = InProcessServerBuilder.generateName();
        Server server1, server2;
        ManagedChannel channel1, channel2;

        long shardId1 = 1L;
        long shardId2 = 2L;
        @Mock Function<Long, ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubByShardId;
        @Mock ShardManager shardManager;
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

            when(shardManager.getAll()).thenReturn(List.of(shardId1, shardId2));
            responses1.offer(Flux.just(notifications1).concatWith(Flux.never()));
            responses2.offer(Flux.just(notifications2).concatWith(Flux.never()));

            try (var manager = new NotificationManager(stubByShardId, shardManager)) {
                manager.registerCallback(notificationCallback);
                assertThat(manager.start()).succeedsWithin(Duration.ofSeconds(1));
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
