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

import static io.streamnative.oxia.client.api.Notification.KeyModified;
import static java.util.concurrent.CompletableFuture.completedFuture;

import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

@Slf4j
public class NotificationManagerImpl extends GrpcResponseStream implements NotificationManager {
    private final Set<Consumer<Notification>> callbacks = ConcurrentHashMap.newKeySet();
    private CompletableFuture<Void> started;

    public NotificationManagerImpl(
            @NonNull Supplier<ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubFactory) {
        super(stubFactory);
    }

    @Override
    protected CompletableFuture<Void> start(
            ReactorOxiaClientGrpc.ReactorOxiaClientStub stub, Consumer<Disposable> consumer) {
        // TODO filter non-retriables?
        RetryBackoffSpec retrySpec =
                Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                        .doBeforeRetry(signal -> log.warn("Retrying receiving notifications: {}", signal));
        var disposable =
                stub.getNotifications(NotificationsRequest.getDefaultInstance())
                        .doOnError(t -> log.warn("Error receiving notifications", t))
                        .retryWhen(retrySpec)
                        .repeat()
                        .subscribe(this::notify);
        consumer.accept(disposable);
        return completedFuture(null);
    }

    private void notify(NotificationBatch batch) {
        batch.getNotificationsMap().entrySet().stream()
                .map(
                        e -> {
                            var key = e.getKey();
                            var notice = e.getValue();
                            return switch (notice.getType()) {
                                case KEY_CREATED -> new KeyCreated(key, notice.getVersionId());
                                case KEY_MODIFIED -> new KeyModified(key, notice.getVersionId());
                                case KEY_DELETED -> new KeyDeleted(key);
                                default -> null;
                            };
                        })
                .filter(Objects::nonNull)
                .forEach(n -> callbacks.parallelStream().forEach(c -> c.accept(n)));
    }

    public void registerCallback(@NonNull Consumer<Notification> callback) {
        callbacks.add(callback);
    }
}
