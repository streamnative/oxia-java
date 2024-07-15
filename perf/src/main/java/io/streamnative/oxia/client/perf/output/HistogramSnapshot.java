package io.streamnative.oxia.client.perf.output;

public record HistogramSnapshot(double p50, double p95, double p99, double p999, double max) {}
