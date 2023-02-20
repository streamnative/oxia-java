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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.api.Notification.KeyModified;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.OxiaClientImplBase;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ShardNotificationReceiverTest {
    BlockingQueue<Flux<NotificationBatch>> responses = new ArrayBlockingQueue<>(10);

    OxiaClientImplBase serviceImpl =
            new OxiaClientImplBase() {
                @Override
                public Flux<NotificationBatch> getNotifications(Mono<NotificationsRequest> request) {
                    Flux<NotificationBatch> assignments = responses.poll();
                    if (assignments == null) {
                        return Flux.error(Status.RESOURCE_EXHAUSTED.asException());
                    }
                    return assignments;
                }
            };

    String serverName = InProcessServerBuilder.generateName();
    Server server;
    ManagedChannel channel;

    long shardId = 1L;
    String leader = "address";
    @Mock Supplier<ReactorOxiaClientStub> stubFactory;
    @Mock Consumer<Notification> notificationCallback;

    @BeforeEach
    void beforeEach() throws Exception {
        responses.clear();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        var stub = ReactorOxiaClientGrpc.newReactorStub(channel);
        doReturn(stub).when(stubFactory).get();
    }

    @AfterEach
    void afterEach() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void start() {
        var notifications =
                NotificationBatch.newBuilder()
                        .putNotifications("key1", created(1L))
                        .putNotifications("key2", deleted(2L))
                        .putNotifications("key3", modified(3L))
                        .build();
        responses.offer(Flux.just(notifications).concatWith(Flux.never()));
        try (var notificationReceiver =
                new ShardNotificationReceiver(stubFactory, shardId, notificationCallback)) {
            assertThat(notificationReceiver.start()).isCompleted();
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                                verify(notificationCallback).accept(new KeyDeleted("key2"));
                                verify(notificationCallback).accept(new KeyModified("key3", 3L));
                            });
        }
    }

    @Test
    void neverStarts() {
        responses.offer(Flux.never());
        try (var notificationReceiver =
                new ShardNotificationReceiver(stubFactory, shardId, notificationCallback)) {
            assertThat(notificationReceiver.start()).isCompleted();
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback, never()).accept(any());
                            });
        }
    }

    @Test
    public void recoveryFromError() {
        responses.offer(Flux.error(Status.UNAVAILABLE.asException()));
        var notifications =
                NotificationBatch.newBuilder().putNotifications("key1", created(1L)).build();
        responses.offer(Flux.just(notifications).concatWith(Flux.never()));
        try (var notificationReceiver =
                new ShardNotificationReceiver(stubFactory, shardId, notificationCallback)) {
            assertThat(notificationReceiver.start()).isCompleted();
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                            });
        }
    }

    @Test
    public void recoveryFromEndOfStream() {
        responses.offer(Flux.empty());
        var notifications =
                NotificationBatch.newBuilder().putNotifications("key1", created(1L)).build();
        responses.offer(Flux.just(notifications).concatWith(Flux.never()));
        try (var notificationReceiver =
                new ShardNotificationReceiver(stubFactory, shardId, notificationCallback)) {
            assertThat(notificationReceiver.start()).isCompleted();
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                            });
        }
    }

    static io.streamnative.oxia.proto.Notification created(long version) {
        return io.streamnative.oxia.proto.Notification.newBuilder()
                .setType(KEY_CREATED)
                .setVersionId(version)
                .build();
    }

    static io.streamnative.oxia.proto.Notification deleted(long version) {
        return io.streamnative.oxia.proto.Notification.newBuilder()
                .setType(KEY_DELETED)
                .setVersionId(version)
                .build();
    }

    static io.streamnative.oxia.proto.Notification modified(long version) {
        return io.streamnative.oxia.proto.Notification.newBuilder()
                .setType(KEY_MODIFIED)
                .setVersionId(version)
                .build();
    }
}
