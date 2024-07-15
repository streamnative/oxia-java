package io.streamnative.oxia.client.perf.ycsb.output;


import io.streamnative.oxia.client.perf.ycsb.WorkerOptions;

public record BenchmarkReport(
        /* definitions section */
        WorkerOptions definition,
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
