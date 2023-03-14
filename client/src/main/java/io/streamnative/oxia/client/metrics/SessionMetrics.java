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
import io.streamnative.oxia.proto.KeepAliveResponse;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Signal;

@RequiredArgsConstructor(access = PACKAGE)
public class SessionMetrics {

    private final @NonNull Metrics.Histogram keepalive;

    public static @NonNull SessionMetrics create(@NonNull Metrics metrics) {
        var heartbeat = metrics.histogram("oxia_client_session_keepalive", NONE);
        return new SessionMetrics(heartbeat);
    }

    public void recordKeepAlive(@NonNull Signal<KeepAliveResponse> signal) {
        var type = "heartbeat";
        switch (signal.getType()) {
            case ON_NEXT -> keepalive.record(1, attributes(type, true));
            case ON_ERROR -> keepalive.record(1, attributes(type, false));
            default -> {}
        }
    }
}
