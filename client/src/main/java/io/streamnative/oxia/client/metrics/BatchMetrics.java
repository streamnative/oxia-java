/*
 * Copyright © 2022-2024 StreamNative Inc.
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
import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.NONE;
import static io.streamnative.oxia.client.metrics.api.Metrics.attributes;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.metrics.api.Metrics.Histogram;
import java.time.Clock;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class BatchMetrics {
    private final Clock clock;
    private final Histogram timerTotal;
    private final Histogram timerExec;
    private final Histogram size;
    private final Histogram count;

    public static BatchMetrics create(Clock clock, Metrics metrics) {
        var timerTotal = metrics.histogram("oxia_client_batch_total_timer", MILLISECONDS);
        var timerExec = metrics.histogram("oxia_client_batch_exec_timer", MILLISECONDS);
        var size = metrics.histogram("oxia_client_batch_size", BYTES);
        var count = metrics.histogram("oxia_client_batch_requests", NONE);
        return new BatchMetrics(clock, timerTotal, timerExec, size, count);
    }

    public Sample recordWrite() {
        return new Sample("write", clock, timerTotal, timerExec, size, count, clock.millis());
    }

    public Sample recordRead() {
        return new Sample("read", clock, timerTotal, timerExec, size, count, clock.millis());
    }

    @RequiredArgsConstructor(access = PACKAGE)
    public static class Sample {
        private final String type;
        private final Clock clock;
        private final Histogram timerTotal;
        private final Histogram timerExec;
        private final Histogram size;
        private final Histogram count;
        private final long startTotal;
        private long startExec;

        public void startExec() {
            startExec = clock.millis();
        }

        public void stop(Throwable t, long size, long count) {
            var attributes = attributes(type, t);
            var end = clock.millis();
            timerTotal.record(end - startTotal, attributes);
            timerExec.record(end - startExec, attributes);
            this.size.record(size, attributes);
            this.count.record(count, attributes);
        }
    }
}
