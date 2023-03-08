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
package io.streamnative.oxia.client.perf;


import static java.util.concurrent.TimeUnit.NANOSECONDS;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.RateLimiter;
import io.streamnative.oxia.client.OxiaClientBuilder;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

@Slf4j
public class PerfClient {

    private static final List<String> keys = new ArrayList<>();

    private static final LongAdder writeOps = new LongAdder();
    private static final LongAdder readOps = new LongAdder();
    private static final LongAdder writeFailed = new LongAdder();
    private static final LongAdder readFailed = new LongAdder();

    private static final Recorder writeLatency = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);
    private static final Recorder readLatency = new Recorder(TimeUnit.SECONDS.toMicros(120000), 5);

    private static final PerfArguments arguments = new PerfArguments();

    public static void main(String[] args) throws Exception {
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("oxia-java perf");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            System.exit(1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(1);
        }

        AsyncOxiaClient client = new OxiaClientBuilder(arguments.serviceAddr)
                .batchLinger(Duration.ofMillis(arguments.batchLingerMs))
                .maxRequestsPerBatch(arguments.maxRequestsPerBatch)
                .requestTimeout(Duration.ofMillis(arguments.requestTimeoutMs))
                .asyncClient()
                .get();

        for (int i = 0; i < arguments.keysCardinality; i++) {
            keys.add("key-" + i);
        }

        ExecutorService executor = Executors.newCachedThreadPool();

        if (arguments.readPercentage != 100) {
            executor.execute(() -> generateWriteTraffic(client));
        }

        if (arguments.readPercentage != 0) {
            executor.execute(() -> generateReadTraffic(client));
        }

        Histogram writeReportHistogram = null;
        Histogram readReportHistogram = null;

        long oldTime = System.nanoTime();

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            double writeRate = writeOps.sumThenReset() / elapsed;
            double readRate = readOps.sumThenReset() / elapsed;
            double failedWriteRate = writeFailed.sumThenReset() / elapsed;
            double failedReadRate = readFailed.sumThenReset() / elapsed;

            writeReportHistogram = writeLatency.getIntervalHistogram(writeReportHistogram);
            readReportHistogram = readLatency.getIntervalHistogram(readReportHistogram);

            log.info("""
                            Stats - Total ops: {} ops/s - Failed ops: {} ops/s
                               Write ops {} w/s  Latency ms: 50% {} - 95% {} - 99% {} - 99.9% {} - max {}
                               Read  ops {} r/s  Latency ms: 50% {} - 95% {} - 99% {} - 99.9% {} - max {}""",
                    INTFORMAT.format(writeRate + readRate), INTFORMAT.format(failedWriteRate + failedReadRate),

                    INTFORMAT.format(writeRate), DEC.format(writeReportHistogram.getValueAtPercentile(50) / 1000.0),
                    DEC.format(writeReportHistogram.getValueAtPercentile(95) / 1000.0),
                    DEC.format(writeReportHistogram.getValueAtPercentile(99) / 1000.0),
                    DEC.format(writeReportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    DEC.format(writeReportHistogram.getMaxValue() / 1000.0),

                    INTFORMAT.format(readRate), DEC.format(readReportHistogram.getValueAtPercentile(50) / 1000.0),
                    DEC.format(readReportHistogram.getValueAtPercentile(95) / 1000.0),
                    DEC.format(readReportHistogram.getValueAtPercentile(99) / 1000.0),
                    DEC.format(readReportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    DEC.format(readReportHistogram.getMaxValue() / 1000.0)

            );

            writeReportHistogram.reset();
            readReportHistogram.reset();

            oldTime = now;
        }
    }

    private static void generateWriteTraffic(AsyncOxiaClient client) {
        double writeRate = arguments.requestsRate * (100.0 - arguments.readPercentage) / 100;
        RateLimiter limiter = RateLimiter.create(writeRate);

        byte[] value = new byte[arguments.valueSize];
        Random rand = new Random();

        while (true) {
            limiter.acquire();

            String key = keys.get(rand.nextInt(keys.size()));

            long start = System.nanoTime();
            client.put(key, value).thenRun(() -> {
                writeOps.increment();
                long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                writeLatency.recordValue(latencyMicros);
            }).exceptionally(ex -> {
                log.warn("Write operation failed {}", ex.getMessage());
                writeFailed.increment();
                return null;
            });
        }
    }

    private static void generateReadTraffic(AsyncOxiaClient client) {
        double readRate = arguments.requestsRate * arguments.readPercentage / 100;
        RateLimiter limiter = RateLimiter.create(readRate);

        Random rand = new Random();

        while (true) {
            limiter.acquire();

            String key = keys.get(rand.nextInt(keys.size()));

            long start = System.nanoTime();
            client.get(key).thenRun(() -> {
                readOps.increment();
                long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
                readLatency.recordValue(latencyMicros);
            }).exceptionally(ex -> {
                log.warn("Read operation failed {}", ex.getMessage());
                readFailed.increment();
                return null;
            });
        }
    }


    static final DecimalFormat THROUGHPUTFORMAT = new PaddingDecimalFormat("0.0", 8);
    static final DecimalFormat DEC = new PaddingDecimalFormat("0.000", 7);
    static final DecimalFormat INTFORMAT = new PaddingDecimalFormat("0", 7);
}
