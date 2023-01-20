package io.streamnative.oxia.client.batch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static lombok.AccessLevel.PACKAGE;

import java.time.Clock;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
class Batcher implements Runnable, AutoCloseable {
    record Config(
            String name,
            long shardId,
            long lingerMs,
            int maxRequestsPerBatch,
            int operationQueueCapacity) {}

    @NonNull private final Config config;
    @NonNull private final Function<Long, Batch> batchFactory;
    @NonNull private final BlockingQueue<Operation> operations;
    @NonNull private final Clock clock;
    private volatile boolean closed;

    Batcher(@NonNull Config config, @NonNull Function<Long, Batch> batchFactory) {
        this(
                config,
                batchFactory,
                new ArrayBlockingQueue<>(config.operationQueueCapacity),
                Clock.systemUTC());
    }

    @Override
    public void run() {
        Batch batch = null;
        var lingerBudgetMs = -1L;
        while (!closed) {
            try {
                Operation operation = null;
                if (batch == null) {
                    operation = operations.poll();
                } else {
                    operation = operations.poll(lingerBudgetMs, MILLISECONDS);
                    var spentLingerBudgetMs = Math.max(0, clock.millis() - batch.getStartTime());
                    lingerBudgetMs = Math.max(0L, lingerBudgetMs - spentLingerBudgetMs);
                }

                if (operation != null) {
                    if (batch == null) {
                        batch = batchFactory.apply(config.shardId);
                        lingerBudgetMs = config.lingerMs;
                    }
                    batch.add(operation);
                }

                if (batch != null) {
                    if (batch.size() == config.maxRequestsPerBatch || lingerBudgetMs == 0) {
                        batch.complete();
                        batch = null;
                    }
                }
            } catch (InterruptedException e) {
                batch.setFailure(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
    }
}
