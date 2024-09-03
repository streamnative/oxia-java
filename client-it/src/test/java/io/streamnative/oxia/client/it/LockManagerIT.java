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
package io.streamnative.oxia.client.it;

import static java.util.function.Function.identity;

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.streamnative.oxia.client.api.AsyncLock;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.LockManager;
import io.streamnative.oxia.client.api.OptionAutoRevalidate;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.lock.LockManagers;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
public class LockManagerIT {
    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME)
                    .withShards(10)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    private final OpenTelemetry openTelemetry;
    private final InMemoryMetricReader metricReader;

    {
        final Resource resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(ResourceAttributes.SERVICE_NAME, "logical-service-name")));
        metricReader = InMemoryMetricReader.create();
        final SdkMeterProvider sdkMeterProvider =
                SdkMeterProvider.builder().registerMetricReader(metricReader).setResource(resource).build();
        openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();
    }

    @Getter
    @AllArgsConstructor
    static class Counter {
        private int current;
        private final int total;

        public void increment() {
            this.current += 1;
        }
    }

    @Test
    public void testCounterWithSyncLock() throws InterruptedException {
        final String lockKey = UUID.randomUUID().toString();
        @Cleanup("shutdown")
        final ExecutorService service =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final Map<String, AsyncOxiaClient> clients = new ConcurrentHashMap<>();
        final Map<String, LockManager> lockManager = new ConcurrentHashMap<>();
        try {
            final Function<String, AsyncOxiaClient> compute =
                    (threadName) ->
                            OxiaClientBuilder.create(oxia.getServiceAddress())
                                    .clientIdentifier(threadName)
                                    .openTelemetry(openTelemetry)
                                    .asyncClient()
                                    .join();
            final var counter = new Counter(0, 3000);
            final var latch = new CountDownLatch(counter.total);
            for (int i = 0; i < counter.total; i++) {
                service.execute(
                        () -> {
                            final String name = Thread.currentThread().getName();
                            final AsyncOxiaClient client = clients.computeIfAbsent(name, compute);
                            final LockManager lm =
                                    lockManager.computeIfAbsent(
                                            name,
                                            (n) ->
                                                    LockManagers.createLockManager(
                                                            client,
                                                            openTelemetry,
                                                            Executors.newSingleThreadScheduledExecutor(
                                                                    new DefaultThreadFactory("oxia-lock-manager")),
                                                            OptionAutoRevalidate.DEFAULT));
                            final AsyncLock lock = lm.getLightWeightLock(lockKey);
                            lock.lock().join();
                            counter.increment();
                            lock.unlock().join();
                            log.info("counter : {}", counter.current);
                            latch.countDown();
                        });
            }

            latch.await();
            Assertions.assertEquals(counter.current, counter.total);
            metricReader.forceFlush();
            var metrics = metricReader.collectAllMetrics();
            var metricsByName =
                    metrics.stream().collect(Collectors.toMap(MetricData::getName, identity()));
            System.out.println(metricsByName);
            Assertions.assertTrue(metricsByName.containsKey("oxia.locks.status"));
        } finally {
            clients.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close oxia client failed", e);
                        }
                    });
        }
    }

    @Test
    public void testCounterWithAsyncLock() throws InterruptedException {
        final String lockKey = UUID.randomUUID().toString();
        @Cleanup("shutdown")
        final ExecutorService service =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final Map<String, AsyncOxiaClient> clients = new ConcurrentHashMap<>();
        try {
            final Function<String, AsyncOxiaClient> compute =
                    (threadName) ->
                            OxiaClientBuilder.create(oxia.getServiceAddress())
                                    .clientIdentifier(threadName)
                                    .openTelemetry(openTelemetry)
                                    .asyncClient()
                                    .join();
            final var counter = new Counter(0, 3000);
            final var latch = new CountDownLatch(counter.total);
            for (int i = 0; i < counter.total; i++) {
                service.execute(
                        () -> {
                            final String name = Thread.currentThread().getName();
                            final AsyncOxiaClient client = clients.computeIfAbsent(name, compute);
                            final AsyncLock lm =
                                    LockManagers.createLockManager(
                                                    client,
                                                    openTelemetry,
                                                    Executors.newSingleThreadScheduledExecutor(
                                                            new DefaultThreadFactory("oxia-lock-manager")),
                                                    OptionAutoRevalidate.DEFAULT)
                                            .getLightWeightLock(lockKey);
                            lm.lock()
                                    .thenAccept(
                                            __ -> {
                                                counter.increment();
                                                log.info("counter : {}", counter.current);
                                            })
                                    .thenCompose(__ -> lm.unlock())
                                    .thenAccept(__ -> latch.countDown())
                                    .exceptionally(
                                            ex -> {
                                                Assertions.fail("unexpected exception", ex);
                                                return null;
                                            });
                        });
            }
            latch.await();
            Assertions.assertEquals(counter.current, counter.total);
            metricReader.forceFlush();
            var metrics = metricReader.collectAllMetrics();
            var metricsByName =
                    metrics.stream().collect(Collectors.toMap(MetricData::getName, identity()));
            System.out.println(metricsByName);
            Assertions.assertTrue(metricsByName.containsKey("oxia.locks.status"));
        } finally {
            clients.forEach(
                    (s, c) -> {
                        try {
                            c.close();
                        } catch (Exception e) {
                            log.error("close oxia client failed", e);
                        }
                    });
        }
    }
}
