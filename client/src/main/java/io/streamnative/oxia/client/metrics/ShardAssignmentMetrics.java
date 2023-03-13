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

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class ShardAssignmentMetrics {

    private final @NonNull Metrics.Histogram lifecycle;
    private final @NonNull Metrics.Histogram changes;

    public static @NonNull ShardAssignmentMetrics create(@NonNull Metrics metrics) {
        var lifecycle = metrics.histogram("oxia_client_shard_assignments_lifecycle", NONE);
        var changes = metrics.histogram("oxia_client_shard_assignments_received", NONE);
        return new ShardAssignmentMetrics(lifecycle, changes);
    }

    public void recordError() {
        recordAssignments(false);
    }

    public void recordRetry() {
        lifecycle.record(1, attributes("retry"));
    }

    @VisibleForTesting
    void recordAssignments(boolean success) {
        var attributes =
                Map.of("type", "assignments", "success", Boolean.toString(success).toLowerCase());
        lifecycle.record(1, attributes);
    }

    public void recordAssignments(@NonNull ShardAssignmentChanges changes) {
        recordAssignments(true);
        this.changes.record(changes.added().size(), attributes("added"));
        this.changes.record(changes.removed().size(), attributes("removed"));
        this.changes.record(changes.reassigned().size(), attributes("reassigned"));
    }
}
