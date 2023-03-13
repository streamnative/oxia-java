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

import static io.streamnative.oxia.client.ProtoUtil.uint32ToLong;
import static io.streamnative.oxia.client.metrics.api.Metrics.Histogram;
import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.NONE;
import static java.util.stream.Collectors.groupingBy;
import static lombok.AccessLevel.PACKAGE;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.proto.Notification;
import io.streamnative.oxia.proto.NotificationBatch;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class NotificationMetrics {

    private final @NonNull Histogram lifecycle;
    private final @NonNull Histogram received;

    public static @NonNull NotificationMetrics create(@NonNull Metrics metrics) {
        var lifecycle = metrics.histogram("oxia_client_notifications_lifecycle", NONE);
        var received = metrics.histogram("oxia_client_notifications_received", NONE);
        return new NotificationMetrics(lifecycle, received);
    }

    @VisibleForTesting
    void recordBatch(long shardId, boolean success) {
        var attributes =
                Map.of(
                        "type", "batch",
                        "shard_id", Long.toString(shardId),
                        "success", Boolean.toString(success).toLowerCase());
        lifecycle.record(1, attributes);
    }

    public void recordError(long shardId) {
        recordBatch(shardId, false);
    }

    public void recordRetry(long shardId) {
        var attributes = Map.of("type", "retry", "shard_id", Long.toString(shardId));
        lifecycle.record(1, attributes);
    }

    public void recordNotifications(@NonNull NotificationBatch batch) {
        var shardId = uint32ToLong(batch.getShardId());
        recordBatch(shardId, true);
        batch.getNotificationsMap().values().stream()
                .collect(groupingBy(Notification::getType))
                .forEach(
                        (key, value) -> {
                            var attributes =
                                    Map.of(
                                            "type", key.name().toLowerCase(),
                                            "shard_id", Long.toString(shardId));
                            received.record(value.size(), attributes);
                        });
    }
}
