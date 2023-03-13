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
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.metrics.api.Metrics;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class SessionMetrics {

    private final @NonNull Metrics.Histogram lifecycle;

    public static @NonNull SessionMetrics create(@NonNull Metrics metrics) {
        var lifecycle = metrics.histogram("oxia_client_session_lifecycle", NONE);
        return new SessionMetrics(lifecycle);
    }

    public void recordKeepAliveError(long shardId) {
        lifecycle.record(1, attributes("heartbeat_error", shardId));
    }

    public void recordKeepAliveRetry(long shardId) {
        lifecycle.record(1, attributes("heartbeat_retry", shardId));
    }

    public void recordCreate(long shardId) {
        lifecycle.record(1, attributes("create", shardId));
    }

    public void recordCloseError(long shardId) {
        lifecycle.record(1, attributes("close_error", shardId));
    }

    private static Map<String, String> attributes(@NonNull String type, long shardId) {
        return Map.of("type", type, "shard_id", Long.toString(shardId));
    }
}
