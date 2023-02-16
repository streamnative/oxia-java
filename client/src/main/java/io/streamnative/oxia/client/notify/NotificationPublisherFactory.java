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


import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

@RequiredArgsConstructor
@Slf4j
public class NotificationPublisherFactory {
    private final @NonNull Supplier<ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubFactory;

    public Publisher<Notification> newPublisher() {
        RetryBackoffSpec retrySpec =
                Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                        .doBeforeRetry(signal -> log.warn("Retrying receiving notifications: {}", signal));
        return JdkFlowAdapter.publisherToFlowPublisher(
                stubFactory
                        .get()
                        .getNotifications(NotificationsRequest.getDefaultInstance())
                        .doOnError(t -> log.warn("Error receiving notifications", t))
                        .retryWhen(retrySpec)
                        .repeat()
                        .flatMap(b -> Flux.fromIterable(b.getNotificationsMap().entrySet()))
                        .map(NotificationPublisherFactory::fromProtoFn)
                        .filter(Objects::nonNull));
    }

    static Notification fromProtoFn(Map.Entry<String, io.streamnative.oxia.proto.Notification> e) {
        var key = e.getKey();
        var notice = e.getValue();
        return switch (notice.getType()) {
            case KEY_CREATED -> new Notification.KeyCreated(key, notice.getVersionId());
            case KEY_MODIFIED -> new Notification.KeyModified(key, notice.getVersionId());
            case KEY_DELETED -> new Notification.KeyDeleted(key);
            default -> null;
        };
    }
}
