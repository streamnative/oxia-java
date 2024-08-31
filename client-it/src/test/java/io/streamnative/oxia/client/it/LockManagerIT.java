package io.streamnative.oxia.client.it;

import io.streamnative.oxia.client.api.AsyncLock;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.LockManager;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Function;

@Slf4j
@Testcontainers
public class LockManagerIT {
    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME)
                    .withShards(10)
                    .withLogConsumer(new Slf4jLogConsumer(log));


    @Test
    public void testCounter() throws InterruptedException {
        @Getter
        @AllArgsConstructor
        class Counter {
            private int current;
            private final int total;

            public void increment() {
                this.current += 1;
            }
        }
        final String lockKey = UUID.randomUUID().toString();

        final ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final Map<String, AsyncOxiaClient> clients = new ConcurrentHashMap<>();
        try {
            final Function<String, AsyncOxiaClient> compute = (threadName) -> OxiaClientBuilder.create(oxia.getServiceAddress())
                    .clientIdentifier(threadName)
                    .asyncClient().join();
            final var counter = new Counter(0, 10);
            final var latch = new CountDownLatch(counter.total);

            for (int i = 0; i < counter.total; i++) {
                service.execute(() -> {
                    final AsyncOxiaClient client = clients.computeIfAbsent(Thread.currentThread().getName(), compute);
                    final AsyncLock lock = client.getLockManager().getLock(lockKey);
                    lock.lock().join();
                    counter.increment();
                    lock.unlock().join();
                    latch.countDown();
                });
            }

            latch.await();
            Assertions.assertEquals(counter.current, counter.total);
        } finally {
            clients.forEach((s, c) -> {
                try {
                    c.close();
                } catch (Exception e) {
                    log.error("close oxia client failed", e);
                }
            });
        }
    }
}
