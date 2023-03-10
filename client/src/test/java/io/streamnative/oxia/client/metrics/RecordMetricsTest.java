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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.streamnative.oxia.client.metrics.api.Metrics;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecordMetricsTest {

    @Mock Metrics metrics;
    @Mock Metrics.Histogram cacheLoadTimer;
    @Mock Metrics.Histogram cacheHits;
    @Mock Metrics.Histogram cacheEvictions;

    @Nested
    @DisplayName("Caffeine record cache tests")
    class CacheMetricsTest {

        StatsCounter statsCounter;

        @BeforeEach
        void beforeEach() {
            when(metrics.histogram("oxia_client_record_cache_load_timer", MILLISECONDS))
                    .thenReturn(cacheLoadTimer);
            when(metrics.histogram("oxia_client_record_cache_hits", NONE)).thenReturn(cacheHits);
            when(metrics.histogram("oxia_client_record_cache_evictions", NONE))
                    .thenReturn(cacheEvictions);

            statsCounter = RecordMetrics.create(metrics).caffeineStatsCounter();
        }

        @Test
        void recordHits() {
            statsCounter.recordHits(2);
            verify(cacheHits).record(2, attributes("cache_hit", true));
        }

        @Test
        void recordMisses() {
            statsCounter.recordMisses(4);
            verify(cacheHits).record(4, attributes("cache_hit", false));
        }

        @Test
        void recordLoadSuccess() {
            statsCounter.recordLoadSuccess(20L);
            verify(cacheLoadTimer).record(20L, attributes("cache_load", true));
        }

        @Test
        void recordLoadFailure() {
            statsCounter.recordLoadFailure(40L);
            verify(cacheLoadTimer).record(40L, attributes("cache_load", false));
        }

        @Test
        void recordEviction() {
            statsCounter.recordEviction(2, RemovalCause.COLLECTED);
            verify(cacheEvictions)
                    .record(
                            2,
                            Map.of(
                                    "type", "cache_eviction",
                                    "result", "success",
                                    "removal_cause", "collected"));
        }

        @Test
        void snapshot() {
            assertThatThrownBy(() -> statsCounter.snapshot())
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
