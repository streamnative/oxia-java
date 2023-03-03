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

import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.BYTES;
import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.MILLISECONDS;
import static io.streamnative.oxia.client.metrics.api.Metrics.attributes;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.metrics.api.Metrics.Histogram;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class OperationMetrics {
    private final Clock clock;
    private final Histogram timer;
    private final Histogram size;

    public static OperationMetrics create(Clock clock, Metrics metrics) {
        var timer = metrics.histogram("oxia_client_operation_timer", MILLISECONDS);
        var size = metrics.histogram("oxia_client_operation_size", BYTES);
        return new OperationMetrics(clock, timer, size);
    }

    public Sample<PutResult> recordPut(long valueSize) {
        return record("put", (t, attributes) -> size.record(valueSize, attributes));
    }

    public Sample<Boolean> recordDelete() {
        return record("delete");
    }

    public Sample<Void> recordDeleteRange() {
        return record("delete_range");
    }

    public Sample<GetResult> recordGet() {
        return record(
                "get",
                (r, attributes) -> {
                    var valueSize = 0;
                    if (r != null) {
                        valueSize = r.getValue().length;
                    }
                    size.record(valueSize, attributes);
                });
    }

    public Sample<List<String>> recordList() {
        return record("list");
    }

    private <R> Sample<R> record(String type) {
        return record(type, (t, attributes) -> {});
    }

    private <R> Sample<R> record(String type, BiConsumer<R, Map<String, String>> consumer) {
        var start = clock.millis();
        return (r, t) -> {
            var attributes = attributes(type, t);
            timer.record(clock.millis() - start, attributes);
            consumer.accept(r, attributes);
        };
    }

    public interface Sample<R> {
        void stop(R result, Throwable t);
    }
}
