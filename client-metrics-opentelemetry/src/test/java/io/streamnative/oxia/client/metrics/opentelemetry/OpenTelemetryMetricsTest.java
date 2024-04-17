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
package io.streamnative.oxia.client.metrics.opentelemetry;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.NONE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongHistogramBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OpenTelemetryMetricsTest {
    @Mock OpenTelemetry openTelemetry;
    @Mock Meter meter;
    @Mock DoubleHistogramBuilder doubleBuilder;
    @Mock LongHistogramBuilder longBuilder;
    @Mock LongHistogram longHistogram;

    @Test
    void test() {
        when(openTelemetry.getMeter("oxia_client")).thenReturn(meter);
        when(meter.histogramBuilder("foo")).thenReturn(doubleBuilder);
        when(doubleBuilder.ofLongs()).thenReturn(longBuilder);
        when(longBuilder.setUnit("1")).thenReturn(longBuilder);
        when(longBuilder.build()).thenReturn(longHistogram);
        when(longBuilder.setExplicitBucketBoundariesAdvice(any())).thenReturn(longBuilder);

        var metrics = OpenTelemetryMetrics.create(openTelemetry);
        var histogram = metrics.histogram("foo", NONE);
        histogram.record(1, Map.of("bar", "baz"));

        verify(longHistogram).record(1, Attributes.of(stringKey("bar"), "baz"));
    }
}
