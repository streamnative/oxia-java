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

import com.fasterxml.jackson.annotation.JsonInclude;
import io.streamnative.oxia.client.perf.ycsb.WorkerOptions;

public record BenchmarkReportSnapshot(
        /* definitions section */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        WorkerOptions definition,
        /* metadata section */
        long timestamp,
        /* ops write section */
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        long totalWrite,
        double writeOps,
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        long totalFailedWrite,
        double writeFps,
        HistogramSnapshot writeLatencyMs,
        /* ops read section */
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        long totalRead,
        double readOps,
        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        long totalFailedRead,
        double readFps,
        HistogramSnapshot readLatencyMs) {}
