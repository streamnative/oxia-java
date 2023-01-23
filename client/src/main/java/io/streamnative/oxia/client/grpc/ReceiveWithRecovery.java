package io.streamnative.oxia.client.grpc;

import static lombok.AccessLevel.PUBLIC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor(access = PUBLIC)
@Slf4j
public class ReceiveWithRecovery implements Receiver {
    private final ScheduledExecutorService executor;
    private final CompletableFuture<Void> closed;
    private final AtomicLong attemptCounter;
    private final LongFunction<Long> retryIntervalFn;
    private final Receiver receiver;

    public ReceiveWithRecovery(@NonNull Receiver receiver) {
        this(
                Executors.newSingleThreadScheduledExecutor(
                        r -> new Thread(r, "shard-manager-assignments-receiver")),
                new CompletableFuture<>(),
                new AtomicLong(),
                new ExponentialBackoff(),
                receiver);
    }

    @Override
    public @NonNull CompletableFuture<Void> receive() {
        executor.execute(this::receiveWithRetry);
        return closed;
    }

    @Override
    public @NonNull CompletableFuture<Void> bootstrap() {
        return receiver.bootstrap();
    }

    private void receiveWithRetry() {
        while (!closed.isDone()) {
            var attempt = attemptCounter.getAndIncrement();
            try {
                if (attempt > 0) {
                    var interval = retryIntervalFn.apply(attempt);
                    Thread.sleep(interval);
                }
                receiver.receive().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                log.error("Shard assignments stream terminated", e.getCause());
            }
        }
    }

    @Override
    public void close() throws Exception {
        closed.complete(null);
        executor.shutdown();
    }
}
