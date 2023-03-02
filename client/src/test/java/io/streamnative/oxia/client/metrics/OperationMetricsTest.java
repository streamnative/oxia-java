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
import static java.util.Optional.empty;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.Version;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.metrics.api.Metrics.Histogram;
import java.time.Clock;
import java.util.List;
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
class OperationMetricsTest {
    @Mock Clock clock;
    @Mock Metrics metrics;
    @Mock Histogram timer;
    @Mock Histogram size;

    OperationMetrics operationMetrics;

    @BeforeEach
    void beforeEach() {
        when(metrics.histogram("oxia_client_operation_timer", MILLISECONDS)).thenReturn(timer);
        when(metrics.histogram("oxia_client_operation_size", BYTES)).thenReturn(size);
        operationMetrics = OperationMetrics.create(clock, metrics);

        when(clock.millis()).thenReturn(1L, 3L);
    }

    @ParameterizedTest
    @MethodSource("args")
    void put(Throwable t, String result) {
        var sample = operationMetrics.recordPut(1L);
        sample.accept(new PutResult(new Version(1, 2, 3, 4, empty(), empty())), t);

        verify(timer).record(2L, Map.of("type", "put", "result", result));
        verify(size).record(1L, Map.of("type", "put", "result", result));
    }

    @ParameterizedTest
    @MethodSource("args")
    void delete(Throwable t, String result) {
        var sample = operationMetrics.recordDelete();
        sample.accept(true, t);

        verify(timer).record(2L, Map.of("type", "delete", "result", result));
        verifyNoInteractions(size);
    }

    @ParameterizedTest
    @MethodSource("args")
    void deleteRange(Throwable t, String result) {
        var sample = operationMetrics.recordDeleteRange();
        sample.accept(null, t);

        verify(timer).record(2L, Map.of("type", "delete_range", "result", result));
        verifyNoInteractions(size);
    }

    @ParameterizedTest
    @MethodSource("args")
    void get(Throwable t, String result) {
        var sample = operationMetrics.recordGet();
        sample.accept(new GetResult(new byte[1], new Version(1, 2, 3, 4, empty(), empty())), t);

        verify(timer).record(2L, Map.of("type", "get", "result", result));
        verify(size).record(1L, Map.of("type", "get", "result", result));
    }

    @ParameterizedTest
    @MethodSource("args")
    void list(Throwable t, String result) {
        var sample = operationMetrics.recordList();
        sample.accept(List.of(), t);

        verify(timer).record(2L, Map.of("type", "list", "result", result));
        verifyNoInteractions(size);
    }

    static Stream<Arguments> args() {
        return Stream.of(Arguments.of(null, "success"), Arguments.of(new Exception(), "failure"));
    }
}
