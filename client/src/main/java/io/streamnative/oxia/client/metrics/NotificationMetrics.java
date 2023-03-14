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
package io.streamnative.oxia.client.metrics;

import static io.streamnative.oxia.client.metrics.api.Metrics.Histogram;
import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.NONE;
import static io.streamnative.oxia.client.metrics.api.Metrics.attributes;
import static java.util.stream.Collectors.groupingBy;
import static lombok.AccessLevel.PACKAGE;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.proto.Notification;
import io.streamnative.oxia.proto.NotificationBatch;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Signal;

@RequiredArgsConstructor(access = PACKAGE)
public class NotificationMetrics {

    private final @NonNull Histogram batch;
    private final @NonNull Histogram event;

    public static @NonNull NotificationMetrics create(@NonNull Metrics metrics) {
        var batch = metrics.histogram("oxia_client_notification", NONE);
        var events = metrics.histogram("oxia_client_notification_event", NONE);
        return new NotificationMetrics(batch, events);
    }

    public void recordBatch(@NonNull Signal<NotificationBatch> signal) {
        var type = "batch";
        switch (signal.getType()) {
            case ON_NEXT -> {
                batch.record(1, attributes(type, true));
                recordNotification(signal.get());
            }
            case ON_ERROR -> batch.record(1, attributes(type, false));
            default -> {}
        }
    }

    @VisibleForTesting
    void recordNotification(@NonNull NotificationBatch batch) {
        batch.getNotificationsMap().values().stream()
                .collect(groupingBy(Notification::getType))
                .forEach(
                        (key, value) -> {
                            var attributes = Map.of("type", key.name().toLowerCase());
                            event.record(value.size(), attributes);
                        });
    }
}
