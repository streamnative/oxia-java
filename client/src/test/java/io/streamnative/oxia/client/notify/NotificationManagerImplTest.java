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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.awaitility.Awaitility.await;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.notify.NotificationManagerImplTest.StreamResponse.Notifications;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NotificationManagerImplTest {

    private BlockingQueue<StreamResponse> responses = new ArrayBlockingQueue<>(10);
    private final ScheduledExecutorService responseSender =
            Executors.newSingleThreadScheduledExecutor();
    private final CompletableFuture<Void> test = new CompletableFuture<>();
    private final AtomicLong getNotificationsCount = new AtomicLong();

    private final OxiaClientGrpc.OxiaClientImplBase serviceImpl =
            mock(
                    OxiaClientGrpc.OxiaClientImplBase.class,
                    delegatesTo(
                            new OxiaClientGrpc.OxiaClientImplBase() {
                                @Override
                                public void getNotifications(
                                        NotificationsRequest request, StreamObserver<NotificationBatch> observer) {
                                    getNotificationsCount.incrementAndGet();
                                    responseSender.execute(
                                            () -> {
                                                var streamDone = false;
                                                var queue = responses;
                                                while (!test.isDone() && !streamDone) {
                                                    try {
                                                        var r = queue.take();
                                                        if (r instanceof StreamResponse.Error e) {
                                                            streamDone = true;
                                                            queue = null;
                                                            observer.onError(e.throwable());
                                                        } else if (r instanceof StreamResponse.Completed c) {
                                                            streamDone = true;
                                                            queue = null;
                                                            observer.onCompleted();
                                                        } else if (r instanceof Notifications a) {
                                                            observer.onNext(a.response);
                                                        }
                                                    } catch (InterruptedException e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            });
                                }
                            }));

    private Function<String, OxiaClientGrpc.OxiaClientStub> clientByShardId;
    private Server server;
    private ManagedChannel channel;
    @Mock Consumer<Notification> notificationConsumer;

    @BeforeEach
    public void setUp() throws Exception {
        responses.clear();
        String serverName = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        clientByShardId = s -> OxiaClientGrpc.newStub(channel);
    }

    @AfterEach
    void tearDown() {
        test.complete(null);
        server.shutdownNow();
        channel.shutdownNow();
        responseSender.shutdownNow();
    }

    @Test
    public void start() throws Exception {
        try (var notificationManager =
                new NotificationManagerImpl("address", clientByShardId, notificationConsumer)) {
            assertThatNoException().isThrownBy(notificationManager::start);
        }
    }

    @Test
    public void notifications() throws Exception {
        try (var notificationManager =
                new NotificationManagerImpl("address", clientByShardId, notificationConsumer)) {
            notificationManager.start();
            responses.add(addDefaultNotification());
            await("consumption of notifications")
                    .untilAsserted(
                            () -> {
                                verify(notificationConsumer).accept(new Notification.KeyCreated("key1", 1L));
                                verify(notificationConsumer).accept(new Notification.KeyDeleted("key2", 2L));
                                verify(notificationConsumer).accept(new Notification.KeyModified("key3", 3L));
                            });
        }
    }

    @Test
    public void recoveryFromError() throws Exception {
        try (var notificationManager =
                new NotificationManagerImpl("address", clientByShardId, notificationConsumer)) {
            notificationManager.start();
            responses.add(addDefaultNotification());
            await("next request").untilAsserted(() -> assertThat(getNotificationsCount).hasValue(1));
            responses.add(error());
            await("next request").untilAsserted(() -> assertThat(getNotificationsCount).hasValue(2));
        }
    }

    @Test
    public void recoveryFromEndOfStream() throws Exception {
        try (var notificationManager =
                new NotificationManagerImpl("address", clientByShardId, notificationConsumer)) {
            notificationManager.start();
            responses.add(addDefaultNotification());
            await("next request").untilAsserted(() -> assertThat(getNotificationsCount).hasValue(1));
            responses.add(completed());
            await("next request").untilAsserted(() -> assertThat(getNotificationsCount).hasValue(2));
        }
    }

    sealed interface StreamResponse
            permits StreamResponse.Completed, Notifications, StreamResponse.Error {
        record Error(Throwable throwable) implements StreamResponse {}

        enum Completed implements StreamResponse {
            INSTANCE;
        }

        record Notifications(NotificationBatch response) implements StreamResponse {}
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

    static io.streamnative.oxia.proto.Notification modification(long version) {
        return io.streamnative.oxia.proto.Notification.newBuilder()
                .setType(KEY_MODIFIED)
                .setVersionId(version)
                .build();
    }

    static void addCreate(NotificationBatch.Builder builder, String key, long version) {
        builder.putNotifications(key, created(version));
    }

    static void addDelete(NotificationBatch.Builder builder, String key, long version) {
        builder.putNotifications(key, deleted(version));
    }

    static void addModification(NotificationBatch.Builder builder, String key, long version) {
        builder.putNotifications(key, modification(version));
    }

    static Notifications addDefaultNotification() {
        NotificationBatch.Builder builder = NotificationBatch.newBuilder();
        addCreate(builder, "key1", 1L);
        addDelete(builder, "key2", 2L);
        addModification(builder, "key3", 3L);
        return new Notifications(builder.build());
    }

    static StreamResponse.Completed completed() {
        return StreamResponse.Completed.INSTANCE;
    }

    static StreamResponse.Error error() {
        return new StreamResponse.Error(new RuntimeException());
    }
}
