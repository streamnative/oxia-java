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

import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.MILLISECONDS;
import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.NONE;
import static io.streamnative.oxia.client.metrics.api.Metrics.attributes;
import static lombok.AccessLevel.PACKAGE;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.metrics.api.Metrics.Histogram;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.index.qual.NonNegative;

@RequiredArgsConstructor(access = PACKAGE)
public class CacheMetrics implements StatsCounter {
    private final Histogram loadTimer;
    private final Histogram hits;
    private final Histogram evictions;

    public static CacheMetrics create(Metrics metrics) {
        var loadTimer = metrics.histogram("oxia_client_cache_load_timer", MILLISECONDS);
        var hits = metrics.histogram("oxia_client_cache_hits", NONE);
        var evictions = metrics.histogram("oxia_client_cache_evictions", NONE);
        return new CacheMetrics(loadTimer, hits, evictions);
    }

    @Override
    public void recordHits(@NonNegative int count) {
        hits.record(count, attributes("hit", true));
    }

    @Override
    public void recordMisses(@NonNegative int count) {
        hits.record(count, attributes("hit", false));
    }

    @Override
    public void recordLoadSuccess(@NonNegative long loadTime) {
        loadTimer.record(loadTime, attributes("load", true));
    }

    @Override
    public void recordLoadFailure(@NonNegative long loadTime) {
        loadTimer.record(loadTime, attributes("load", false));
    }

    @Override
    public void recordEviction(@NonNegative int weight, RemovalCause removalCause) {
        var attributes = new HashMap<>(attributes("eviction"));
        attributes.put(
                "removal_cause", removalCause == null ? "unknown" : removalCause.name().toLowerCase());
        evictions.record(weight, Map.copyOf(attributes));
    }

    @Override
    public CacheStats snapshot() {
        throw new UnsupportedOperationException(
                "Metrics should be observed via the Oxia client metrics subsystem");
    }
}
