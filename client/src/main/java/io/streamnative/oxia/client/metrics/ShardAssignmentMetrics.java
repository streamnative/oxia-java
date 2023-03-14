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

import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.NONE;
import static io.streamnative.oxia.client.metrics.api.Metrics.attributes;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.streamnative.oxia.proto.ShardAssignments;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Signal;

@RequiredArgsConstructor(access = PACKAGE)
public class ShardAssignmentMetrics {

    private final @NonNull Metrics.Histogram event;
    private final @NonNull Metrics.Histogram change;

    public static @NonNull ShardAssignmentMetrics create(@NonNull Metrics metrics) {
        var event = metrics.histogram("oxia_client_shard_assignment", NONE);
        var change = metrics.histogram("oxia_client_shard_assignment_change", NONE);
        return new ShardAssignmentMetrics(event, change);
    }

    public void recordAssignments(@NonNull Signal<ShardAssignments> signal) {
        var type = "event";
        switch (signal.getType()) {
            case ON_NEXT -> event.record(1, attributes(type, true));
            case ON_ERROR -> event.record(1, attributes(type, false));
            default -> {}
        }
    }

    public void recordChanges(@NonNull ShardAssignmentChanges changes) {
        change.record(changes.added().size(), Map.of("type", "added"));
        change.record(changes.removed().size(), Map.of("type", "removed"));
        change.record(changes.reassigned().size(), Map.of("type", "reassigned"));
    }
}
