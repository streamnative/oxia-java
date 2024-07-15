package io.streamnative.oxia.client.perf.ycsb.output;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

public record HistogramSnapshot(
    double p50,
    double p95,
    double p99,
    double p999,
    double max
) {


  public static HistogramSnapshot fromHistogram(Recorder recorder) {
    final Histogram histogram = recorder.getIntervalHistogram();
    return new HistogramSnapshot(
        histogram.getValueAtPercentile(50) / 1000.0,
        histogram.getValueAtPercentile(95) / 1000.0,
        histogram.getValueAtPercentile(99) / 1000.0,
        histogram.getValueAtPercentile(999) / 1000.0,
        histogram.getMaxValue() / 1000.0
    );
  }
}
