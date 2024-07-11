package io.streamnative.oxia.client.perf.output;

import io.streamnative.oxia.client.perf.PerfArguments;

public record BenchmarkReport(
        /* definitions section */
        PerfArguments template,
        /* metadata section */
        long timestamp,
        /* ops write section */
        long totalWrite,
        double writeOps,
        long totalFailedWrite,
        double writeFps,
        HistogramSnapshot writeLatency,
        /* ops read section */
        long totalRead,
        double readOps,
        long totalFailedRead,
        double readFps,
        HistogramSnapshot readLatency
) {

}
