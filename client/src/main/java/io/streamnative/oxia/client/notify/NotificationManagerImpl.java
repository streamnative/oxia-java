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

import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

@RequiredArgsConstructor
@Slf4j
public class NotificationManagerImpl implements NotificationManager {
    private final @NonNull Function<String, ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubFactory;
    private final @NonNull String serviceAddress;
    private final @NonNull Consumer<Notification> notificationCallback;
    private volatile Disposable disposable;

    @Override
    public CompletableFuture<Void> start() {
        synchronized (this) {
            if (disposable != null) {
                throw new IllegalStateException("Already started");
            }
            // TODO filter non-retriables?
            RetryBackoffSpec retrySpec =
                    Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                            .doBeforeRetry(signal -> log.warn("Retrying receiving notifications: {}", signal));
            var assignmentsFlux =
                    stubFactory
                            .apply(serviceAddress)
                            .getNotifications(NotificationsRequest.getDefaultInstance())
                            .doOnError(t -> log.warn("Error receiving notifications", t))
                            .retryWhen(retrySpec)
                            .repeat()
                            .doOnNext(this::notify)
                            .publish();
            // Complete after the first response has been processed
            var future = Mono.from(assignmentsFlux).then().toFuture();
            disposable = assignmentsFlux.connect();
            return future;
        }
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
                .forEach(notificationCallback);
    }

    @Override
    public void close() {
        if (disposable != null) {
            synchronized (this) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }
    }
}
