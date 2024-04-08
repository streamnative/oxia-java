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

import static lombok.AccessLevel.PACKAGE;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.streamnative.oxia.client.metrics.api.Metrics;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class OpenTelemetryMetrics implements Metrics {
    private final Meter meter;

    public static Metrics create(OpenTelemetry openTelemetry) {
        var meter = openTelemetry.getMeter("oxia_client");
        return new OpenTelemetryMetrics(meter);
    }

    @Override
    public Histogram histogram(String name, Unit unit) {
        var histogram = meter.histogramBuilder(name)
                .ofLongs()
                .setExplicitBucketBoundariesAdvice(List.of(0L, 1L, 2L, 5L, 10L, 20L, 30L, 50L, 75L, 100L, 200L, 500L, 1_000L, 10_000L, 30_000L, 60_000L))
                .setUnit(unit(unit))
                .build();
        return (value, attributes) -> histogram.record(value, attributes(attributes));
    }

    private String unit(Unit unit) {
        return switch (unit) {
            case BYTES -> "By";
            case MILLISECONDS -> "ms";
            default -> "1";
        };
    }

    private Attributes attributes(Map<String, String> attributes) {
        var builder = Attributes.builder();
        attributes.forEach(builder::put);
        return builder.build();
    }
}
