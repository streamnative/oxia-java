package io.streamnative.oxia.client.perf;

import com.google.common.util.concurrent.RateLimiter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.exceptions.OxiaException;
import io.streamnative.oxia.client.perf.generator.*;
import io.streamnative.oxia.client.perf.output.BenchmarkReport;
import io.streamnative.oxia.client.perf.output.Output;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Recorder;
import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Slf4j
public final class Worker implements Runnable, Closeable, Operations {
  private final SyncOxiaClient client;
  private final Generator<String> keyGenerator;
  private final Generator<byte[]> valueGenerator;
  private final Generator<OperationType> operationGenerator;
  private final Output output;

  private volatile CompletableFuture<Void> closeFuture;

  public Worker(PerfArguments arguments) {
    final AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder()
        .build();
    try {
      this.client = OxiaClientBuilder.create(arguments.serviceAddr)
          .batchLinger(Duration.ofMillis(arguments.batchLingerMs))
          .maxRequestsPerBatch(arguments.maxRequestsPerBatch)
          .requestTimeout(Duration.ofMillis(arguments.requestTimeoutMs))
          .namespace(arguments.namespace)
          .openTelemetry(sdk.getOpenTelemetrySdk())
          .syncClient();
    } catch (OxiaException e) {
      throw new WorkerException(e);
    }

    this.keyGenerator = Generators.createKeyGenerator(new KeyGeneratorOptions());
    this.valueGenerator = Generators.createValueGenerator();
    this.operationGenerator = Generators.createOperationGenerator(new OperationGeneratorOptions());
    this.output = Outputs.createLogOutput();
  }


  @SuppressWarnings("UnstableApiUsage")
  @Override
  public void run() {
    final RateLimiter operationRatelimiter = RateLimiter.create(1);
    final Semaphore outstandingSemaphore = new Semaphore(1);

    final LongAdder writeOps = new LongAdder();
    final LongAdder writeFailed = new LongAdder();
    final LongAdder readOps = new LongAdder();
    final LongAdder readFailed = new LongAdder();
    final Recorder writeLatency = new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5);
    final Recorder readLatency = new Recorder(TimeUnit.SECONDS.toMicros(120_000), 5);


    while (closeFuture == null) {
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
            writeOps.increment();
            final long start = System.nanoTime();
            final Status sts = write(key, valueGenerator.nextValue());
            if (!sts.isSuccess()) {
              writeFailed.increment();
            } else {
              final long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
              writeLatency.recordValue(latencyMicros);
            }
          }
          case READ -> {
            readOps.increment();
            final long start = System.nanoTime();
            final Status sts = read(key);
            if (!sts.isSuccess()) {
              readFailed.increment();
            } else {
              final long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
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
      outstandingSemaphore.acquire(1); // acquire all of permits
    } catch (InterruptedException e) {
      throw new WorkerException(e);
    }

    new BenchmarkReport(, )
    output.report();

    if (closeFuture != null && closeFuture.complete(null)) {
      log.warn("bug! unexpected behaviour: completed future and empty close future");
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
