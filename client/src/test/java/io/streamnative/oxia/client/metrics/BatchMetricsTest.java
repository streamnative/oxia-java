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
import static io.streamnative.oxia.client.metrics.api.Metrics.Unit.NONE;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.metrics.api.Metrics.Histogram;
import java.time.Clock;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatchMetricsTest {
    @Mock Clock clock;
    @Mock Metrics metrics;
    @Mock Histogram timerTotal;
    @Mock Histogram timerExec;
    @Mock Histogram size;
    @Mock Histogram count;

    BatchMetrics batchMetrics;

    @BeforeEach
    void beforeEach() {
        when(metrics.histogram("oxia_client_batch_total_timer", MILLISECONDS)).thenReturn(timerTotal);
        when(metrics.histogram("oxia_client_batch_exec_timer", MILLISECONDS)).thenReturn(timerExec);
        when(metrics.histogram("oxia_client_batch_size", BYTES)).thenReturn(size);
        when(metrics.histogram("oxia_client_batch_requests", NONE)).thenReturn(count);

        batchMetrics = BatchMetrics.create(clock, metrics);

        when(clock.millis()).thenReturn(1L, 3L, 4L);
    }

    @ParameterizedTest
    @MethodSource("args")
    void write(Throwable t, String result) {
        var sample = batchMetrics.recordWrite();
        sample.startExec();
        sample.stop(t, 5, 4);

        verify(timerTotal).record(3L, Map.of("type", "write", "result", result));
        verify(timerExec).record(1L, Map.of("type", "write", "result", result));
        verify(size).record(5L, Map.of("type", "write", "result", result));
        verify(count).record(4L, Map.of("type", "write", "result", result));
    }

    @ParameterizedTest
    @MethodSource("args")
    void read(Throwable t, String result) {
        var sample = batchMetrics.recordRead();
        sample.startExec();
        sample.stop(t, 5, 4);

        verify(timerTotal).record(3L, Map.of("type", "read", "result", result));
        verify(timerExec).record(1L, Map.of("type", "read", "result", result));
        verify(size).record(5L, Map.of("type", "read", "result", result));
        verify(count).record(4L, Map.of("type", "read", "result", result));
    }

    static Stream<Arguments> args() {
        return Stream.of(Arguments.of(null, "success"), Arguments.of(new Exception(), "failure"));
    }
}
