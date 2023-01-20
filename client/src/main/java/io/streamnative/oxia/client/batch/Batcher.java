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
    @NonNull private final String name;
    private final long linger;
    private final long maxPollDuration;
    private final int maxRequestsPerBatch;
    private final int operationQueueCapacity;
    @NonNull private final Function<Long, Batch> batchFactory;
    private final long shardId;
    private final BlockingQueue<Operation> operations =
            new ArrayBlockingQueue<>(operationQueueCapacity);
    private volatile boolean closed;
    private final Clock clock;

    @Override
    public void run() {
        Batch batch = null;
        var startPoll = -1L;
        var lingerBudget = -1L;
        while (!closed) {
            try {
                Operation operation = null;
                if (batch == null) {
                    operation = operations.poll();
                    lingerBudget = linger;
                } else {
                    startPoll = clock.millis();
                    operation = operations.poll(lingerBudget, MILLISECONDS);
                    var pollDuration = Math.max(0, clock.millis() - startPoll);
                    lingerBudget = Math.max(0L, lingerBudget - pollDuration);
                }

                if (operation != null) {
                    if (batch == null) {
                        batch = batchFactory.apply(shardId);
                    }
                    batch.add(operation);
                }

                if (batch != null) {
                    if (batch.size() == maxRequestsPerBatch || lingerBudget == 0) {
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
