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

import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.MILLISECONDS;
import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.NONE;
import static io.streamnative.oxia.client.metrics.api.Metrics.attributes;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.RemovalCause;
import io.streamnative.oxia.client.metrics.api.Metrics;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CacheMetricsTest {

    @Mock Metrics metrics;
    @Mock Metrics.Histogram loadTimer;
    @Mock Metrics.Histogram hits;
    @Mock Metrics.Histogram evictions;

    CacheMetrics cacheMetrics;

    @BeforeEach
    void beforeEach() {
        when(metrics.histogram("oxia_client_cache_load_timer", MILLISECONDS)).thenReturn(loadTimer);
        when(metrics.histogram("oxia_client_cache_hits", NONE)).thenReturn(hits);
        when(metrics.histogram("oxia_client_cache_evictions", NONE)).thenReturn(evictions);

        cacheMetrics = CacheMetrics.create(metrics);
    }

    @Test
    void recordHits() {
        cacheMetrics.recordHits(2);
        verify(hits).record(2, attributes("hit", true));
    }

    @Test
    void recordMisses() {
        cacheMetrics.recordMisses(4);
        verify(hits).record(4, attributes("hit", false));
    }

    @Test
    void recordLoadSuccess() {
        cacheMetrics.recordLoadSuccess(20L);
        verify(loadTimer).record(20L, attributes("load", true));
    }

    @Test
    void recordLoadFailure() {
        cacheMetrics.recordLoadFailure(40L);
        verify(loadTimer).record(40L, attributes("load", false));
    }

    @Test
    void recordEviction() {
        cacheMetrics.recordEviction(2, RemovalCause.COLLECTED);
        verify(evictions)
                .record(
                        2,
                        Map.of(
                                "type", "eviction",
                                "result", "success",
                                "removal_cause", "collected"));
    }

    @Test
    void snapshot() {
        assertThatThrownBy(() -> cacheMetrics.snapshot())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
