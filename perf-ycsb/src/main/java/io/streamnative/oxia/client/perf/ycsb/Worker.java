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
package io.streamnative.oxia.client.perf.ycsb;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import com.google.common.util.concurrent.RateLimiter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.exceptions.OxiaException;
import io.streamnative.oxia.client.perf.ycsb.generator.*;
import io.streamnative.oxia.client.perf.ycsb.operations.Operations;
import io.streamnative.oxia.client.perf.ycsb.operations.Status;
import io.streamnative.oxia.client.perf.ycsb.output.BenchmarkReport;
import io.streamnative.oxia.client.perf.ycsb.output.HistogramSnapshot;
import io.streamnative.oxia.client.perf.ycsb.output.Output;
import io.streamnative.oxia.client.perf.ycsb.output.Outputs;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Recorder;
import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;


@Slf4j
public final class Worker implements Runnable, Closeable, Operations {
  private final WorkerOptions options;
  private final SyncOxiaClient client;
  private final Generator<String> keyGenerator;
  private final Generator<byte[]> valueGenerator;
  private final Generator<OperationType> operationGenerator;
  private final Output output;

  private volatile CompletableFuture<Void> closeFuture;

  public Worker(WorkerOptions options) {
    final AutoConfiguredOpenTelemetrySdk sdk =
        AutoConfiguredOpenTelemetrySdk.builder()
            .build();
    try {
      this.client = OxiaClientBuilder.create(options.serviceAddr)
          .batchLinger(Duration.ofMillis(options.batchLingerMs))
          .maxRequestsPerBatch(options.maxRequestsPerBatch)
          .requestTimeout(Duration.ofMillis(options.requestTimeoutMs))
          .namespace(options.namespace)
          .openTelemetry(sdk.getOpenTelemetrySdk())
          .syncClient();
    } catch (OxiaException e) {
      throw new WorkerException(e);
    }
    final GeneratorType generatorType =
        GeneratorType.fromString(options.keyDistribution);

    this.keyGenerator = Generators.createKeyGenerator(
        new KeyGeneratorOptions(generatorType, options.keyPrefix,
            options.bound));
    this.valueGenerator = Generators.createFixedLengthValueGenerator(
        options.valueSize);
    this.operationGenerator =
        Generators.createOperationGenerator(
            new OperationGeneratorOptions(options.writePercentage,
                options.readPercentage, options.scanPercentage));
    this.output = Outputs.createLogOutput();
    this.options = options;
  }


  @SuppressWarnings("UnstableApiUsage")
  @Override
  public void run() {
    final RateLimiter operationRatelimiter =
        RateLimiter.create(options.requestsRate);
    final int maxOutstandingRequests = options.maxOutstandingRequests;
    final Semaphore outstandingSemaphore =
        new Semaphore(maxOutstandingRequests);

    final LongAdder writeTotal = new LongAdder();
    final LongAdder writeFailed = new LongAdder();
    final LongAdder readTotal = new LongAdder();
    final LongAdder readFailed = new LongAdder();
    final Recorder writeLatency =
        new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5);
    final Recorder readLatency =
        new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5);


    final long taskStartTime = System.nanoTime();
    while (closeFuture == null ||
           (options.operationNum > 0 && options.operationNum-- == 0)) {
      // jump out by closing worker
      operationRatelimiter.acquire();
      try {
        outstandingSemaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new WorkerException(e);
      }

      final OperationType operationType = operationGenerator.nextValue();
      final String key = keyGenerator.nextValue();
      Thread.ofVirtual().unstarted(() -> {
        try {
          switch (operationType) {
          case WRITE -> {
            writeTotal.increment();
            final long start = System.nanoTime();
            final Status sts = write(key, valueGenerator.nextValue());
            if (!sts.isSuccess()) {
              writeFailed.increment();
            } else {
              final long latencyMicros =
                  NANOSECONDS.toMicros(System.nanoTime() - start);
              writeLatency.recordValue(latencyMicros);
            }
          }
          case READ -> {
            readTotal.increment();
            final long start = System.nanoTime();
            final Status sts = read(key);
            if (!sts.isSuccess()) {
              readFailed.increment();
            } else {
              final long latencyMicros =
                  NANOSECONDS.toMicros(System.nanoTime() - start);
              readLatency.recordValue(latencyMicros);
            }
          }
          default -> throw new UnsupportedOperationException("unsupported yet");
          }
        } finally {
          outstandingSemaphore.release();
        }
      });
    }

    try {
      outstandingSemaphore.acquire(
          maxOutstandingRequests); // acquire all of permits
    } catch (InterruptedException e) {
      throw new WorkerException(e);
    }

    double elapsed = (System.nanoTime() - taskStartTime) / 1e9;
    // write section
    final long totalWrite = writeTotal.sumThenReset();
    final double writeOps = totalWrite / elapsed;
    final long totalWriteFailed = writeFailed.sumThenReset();
    final double writeFailedOps = totalWriteFailed / elapsed;

    // read section
    final long totalRead = readTotal.sumThenReset();
    final double readOps = totalRead / elapsed;
    final long totalReadFailed = readFailed.sumThenReset();
    final double readFailedOps = totalReadFailed / elapsed;


    final BenchmarkReport snapshot =
        new BenchmarkReport(options, System.currentTimeMillis(), totalWrite,
            writeOps,
            totalWriteFailed, writeFailedOps,
            HistogramSnapshot.fromHistogram(writeLatency), totalRead, readOps,
            totalReadFailed,
            readFailedOps, HistogramSnapshot.fromHistogram(readLatency));
    output.report(snapshot);

    if (closeFuture != null && closeFuture.complete(null)) {
      log.warn(
          "bug! unexpected behaviour: completed future and empty close future");
    }
  }

  @Override
  public void close() {
    // mark the worker is closing
    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          closeFuture = new CompletableFuture<>();
        }
      }
    }
    // wait for task run complete
    closeFuture.join();
    try {
      client.close();
    } catch (Exception ex) {
      throw new WorkerException(ex);
    }
  }

  @Override
  public Status write(String key, byte[] value) {
    try {
      final PutResult result = client.put(key, value);
      if (result != null) {
        return Status.success();
      }
      return Status.failed("empty result");
    } catch (Throwable ex) {
      return Status.failed(ex.getMessage());
    }
  }

  @Override
  public Status read(String key) {
    try {
      final GetResult result = client.get(key);
      if (result != null) {
        return Status.success(result.getValue());
      }
      return Status.failed("empty result");
    } catch (Throwable ex) {
      return Status.failed(ex.getMessage());
    }
  }
}
