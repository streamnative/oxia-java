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
package io.streamnative.oxia.client.perf.ycsb.output;

import org.HdrHistogram.Histogram;

public record HistogramSnapshot(double p50, double p95, double p99, double p999, double max) {

    public static HistogramSnapshot fromHistogram(Histogram histogram) {
        return new HistogramSnapshot(
                histogram.getValueAtPercentile(50) / 1000.0,
                histogram.getValueAtPercentile(95) / 1000.0,
                histogram.getValueAtPercentile(99) / 1000.0,
                histogram.getValueAtPercentile(999) / 1000.0,
                histogram.getMaxValue() / 1000.0);
    }
}
