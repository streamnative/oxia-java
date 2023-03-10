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
public class RecordMetrics {
    private final Histogram cacheLoadTimer;
    private final Histogram cacheHits;
    private final Histogram cacheEvictions;

    public static RecordMetrics create(Metrics metrics) {
        var cacheLoadTimer = metrics.histogram("oxia_client_record_cache_load_timer", MILLISECONDS);
        var cacheHits = metrics.histogram("oxia_client_record_cache_hits", NONE);
        var cacheEvictions = metrics.histogram("oxia_client_record_cache_evictions", NONE);
        return new RecordMetrics(cacheLoadTimer, cacheHits, cacheEvictions);
    }

    public StatsCounter caffeineStatsCounter() {
        return new CaffeineStatsCounter();
    }

    public class CaffeineStatsCounter implements StatsCounter {
        @Override
        public void recordHits(@NonNegative int count) {
            cacheHits.record(count, attributes("cache_hit", true));
        }

        @Override
        public void recordMisses(@NonNegative int count) {
            cacheHits.record(count, attributes("cache_hit", false));
        }

        @Override
        public void recordLoadSuccess(@NonNegative long loadTime) {
            cacheLoadTimer.record(loadTime, attributes("cache_load", true));
        }

        @Override
        public void recordLoadFailure(@NonNegative long loadTime) {
            cacheLoadTimer.record(loadTime, attributes("cache_load", false));
        }

        @Override
        public void recordEviction(@NonNegative int weight, RemovalCause removalCause) {
            var attributes = new HashMap<>(attributes("cache_eviction"));
            attributes.put(
                    "removal_cause", removalCause == null ? "unknown" : removalCause.name().toLowerCase());
            cacheEvictions.record(weight, Map.copyOf(attributes));
        }

        @Override
        public CacheStats snapshot() {
            throw new UnsupportedOperationException(
                    "Metrics should be observed via the Oxia client metrics subsystem");
        }
    }
}
