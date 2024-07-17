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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.exceptions.OxiaException;
import io.streamnative.oxia.client.perf.ycsb.generator.Generator;
import io.streamnative.oxia.client.perf.ycsb.generator.GeneratorType;
import io.streamnative.oxia.client.perf.ycsb.generator.Generators;
import io.streamnative.oxia.client.perf.ycsb.generator.KeyGeneratorOptions;
import io.streamnative.oxia.client.perf.ycsb.generator.OperationGeneratorOptions;
import io.streamnative.oxia.client.perf.ycsb.generator.OperationType;
import io.streamnative.oxia.client.perf.ycsb.operations.Operations;
import io.streamnative.oxia.client.perf.ycsb.operations.Status;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import io.streamnative.oxia.client.perf.ycsb.output.BenchmarkReport;
import io.streamnative.oxia.client.perf.ycsb.output.BenchmarkReportSnapshot;
import io.streamnative.oxia.client.perf.ycsb.output.Output;
import io.streamnative.oxia.client.perf.ycsb.output.Outputs;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Worker implements Runnable, Closeable, Operations {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final WorkerOptions options;
    private final SyncOxiaClient client;
    private final Generator<String> keyGenerator;
    private final Generator<byte[]> valueGenerator;
    private final Generator<OperationType> operationGenerator;
    private final Output intervalOutput;
    private final Output globalOutput;

    private volatile CompletableFuture<Void> closeFuture;

    public Worker(WorkerOptions options) {
        final AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder().build();
        try {
            this.client =
                    OxiaClientBuilder.create(options.serviceAddr)
                            .batchLinger(Duration.ofMillis(options.batchLingerMs))
                            .maxRequestsPerBatch(options.maxRequestsPerBatch)
                            .requestTimeout(Duration.ofMillis(options.requestTimeoutMs))
                            .namespace(options.namespace)
                            .openTelemetry(sdk.getOpenTelemetrySdk())
                            .syncClient();
        } catch (OxiaException e) {
            throw new WorkerException(e);
        }
        final GeneratorType generatorType = GeneratorType.fromString(options.keyDistribution);

        this.keyGenerator =
                Generators.createKeyGenerator(
                        new KeyGeneratorOptions(generatorType, options.keyPrefix, options.lowerBound,
                                options.upperBound, options.elements, options.exponent));
        this.valueGenerator = Generators.createFixedLengthValueGenerator(options.valueSize);
        this.operationGenerator =
                Generators.createOperationGenerator(
                        new OperationGeneratorOptions(
                                options.writePercentage, options.readPercentage, options.scanPercentage));
        this.intervalOutput = Outputs.createLogOutput(false);
        this.globalOutput = Outputs.createLogOutput(true);
        this.options = options;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public void run() {
        try {
            final String optionsStr = MAPPER.writeValueAsString(options);
            log.info("starting worker. the options={}", optionsStr);
        } catch (JsonProcessingException ex) {
            throw new WorkerException(ex);
        }

        final RateLimiter operationRatelimiter = RateLimiter.create(options.requestsRate);
        final int maxOutstandingRequests = options.maxOutstandingRequests;
        final Semaphore outstandingSemaphore = new Semaphore(maxOutstandingRequests);

        final BenchmarkReport globalReport = BenchmarkReport.createDefault();
        final BenchmarkReport intervalReport = BenchmarkReport.createDefault();

        final Function<Long, BenchmarkReportSnapshot> globalSnapshotFunc =
                globalReport.snapshotFunc(options, false);
        final Function<Long, BenchmarkReportSnapshot> internalSnapshotFunc =
                intervalReport.snapshotFunc(options, true);

        log.info("performance test is starting");
        final AtomicLong operationNum = new AtomicLong(options.operationNum);
        final Thread intervalOutputTask =
                Thread.ofVirtual()
                        .start(
                                () -> {
                                    log.info("starting interval output task.");
                                    long lastSnapshotTime = System.nanoTime();
                                    //noinspection InfiniteLoopStatement
                                    while (true) {
                                        try {
                                            //noinspection BusyWait
                                            Thread.sleep(options.intervalOutputSec * 1000L);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            log.info("exit interval output thread while sleeping by interrupt");
                                            return;
                                        }

                                        intervalOutput.report(internalSnapshotFunc.apply(lastSnapshotTime));
                                        if (options.operationNum > 0) {
                                            log.info("remain operation num {}", operationNum.get());
                                        }
                                        lastSnapshotTime = System.nanoTime();
                                    }
                                });
        final long taskStartTime = System.nanoTime();
        while ((options.operationNum > 0 ? operationNum.getAndDecrement() > 0 : closeFuture == null)) {
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
            Thread.ofVirtual()
                    .start(
                            () -> {
                                try {
                                    switch (operationType) {
                                        case WRITE -> {
                                            globalReport.writeTotal().increment();
                                            intervalReport.writeTotal().increment();
                                            final long start = System.nanoTime();
                                            final Status sts = write(key, valueGenerator.nextValue());
                                            if (!sts.isSuccess()) {
                                                globalReport.writeFailed().increment();
                                                intervalReport.writeFailed().increment();
                                            } else {
                                                final long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                                                globalReport.writeLatency().recordValue(latencyMicros);
                                                intervalReport.writeLatency().recordValue(latencyMicros);
                                            }
                                        }
                                        case READ -> {
                                            globalReport.readTotal().increment();
                                            intervalReport.readTotal().increment();
                                            final long start = System.nanoTime();
                                            final Status sts = read(key);
                                            if (!sts.isSuccess()) {
                                                globalReport.readFailed().increment();
                                                intervalReport.readFailed().increment();
                                            } else {
                                                final long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                                                globalReport.readLatency().recordValue(latencyMicros);
                                                intervalReport.readLatency().recordValue(latencyMicros);
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
            outstandingSemaphore.acquire(maxOutstandingRequests); // acquire all of permits
        } catch (InterruptedException e) {
            throw new WorkerException(e);
        }
        final BenchmarkReportSnapshot globalSnapshot = globalSnapshotFunc.apply(taskStartTime);

        // interrupt the interval output task
        intervalOutputTask.interrupt();

        globalOutput.report(globalSnapshot);

        if (closeFuture == null) {
            synchronized (this) {
                if (closeFuture == null) {
                    // avoid close after running
                    closeFuture = CompletableFuture.completedFuture(null);
                }
            }
        } else {
            if (!closeFuture.complete(null)) {
                log.warn("bug! unexpected behaviour: completed future and empty close future");
            }
        }
        log.info("performance test is done");
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
