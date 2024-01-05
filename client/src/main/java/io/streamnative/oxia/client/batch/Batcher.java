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
package io.streamnative.oxia.client.batch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.batch.Operation.CloseOperation;
import io.streamnative.oxia.client.metrics.BatchMetrics;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.time.Clock;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor(access = PACKAGE)
public class Batcher implements Runnable, AutoCloseable {

    private static final int DEFAULT_INITIAL_QUEUE_CAPACITY = 1_000;

    @NonNull private final ClientConfig config;
    private final long shardId;
    @NonNull private final Function<Long, Batch> batchFactory;
    @NonNull private final BlockingQueue<Operation<?>> operations;
    @NonNull private final Clock clock;

    Batcher(@NonNull ClientConfig config, long shardId, @NonNull Function<Long, Batch> batchFactory) {
        this(
                config,
                shardId,
                batchFactory,
                new PriorityBlockingQueue<>(DEFAULT_INITIAL_QUEUE_CAPACITY, Operation.PriorityComparator),
                Clock.systemUTC());
    }

    @SneakyThrows
    public <R> void add(@NonNull Operation<R> operation) {
        try {
            operations.put(operation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        Batch batch = null;
        var lingerBudgetMs = -1L;
        while (true) {
            try {
                Operation<?> operation = null;
                if (batch == null) {
                    operation = operations.take();
                } else {
                    operation = operations.poll(lingerBudgetMs, MILLISECONDS);
                    var spentLingerBudgetMs = Math.max(0, clock.millis() - batch.getStartTime());
                    lingerBudgetMs = Math.max(0L, lingerBudgetMs - spentLingerBudgetMs);
                }

                if (operation == CloseOperation.INSTANCE) {
                    break;
                }

                if (operation != null) {
                    if (batch == null) {
                        batch = batchFactory.apply(shardId);
                        lingerBudgetMs = config.batchLinger().toMillis();
                    }
                    try {
                        if (!batch.canAdd(operation)) {
                            batch.complete();
                            batch = batchFactory.apply(shardId);
                            lingerBudgetMs = config.batchLinger().toMillis();
                        }
                        batch.add(operation);
                    } catch (Exception e) {
                        operation.fail(e);
                    }
                }

                if (batch != null) {
                    if (batch.size() == config.maxRequestsPerBatch() || lingerBudgetMs == 0) {
                        batch.complete();
                        batch = null;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    static @NonNull Function<Long, Batcher> newReadBatcherFactory(
            @NonNull ClientConfig config,
            @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
            Clock clock,
            BatchMetrics metrics) {
        return s ->
                new Batcher(config, s, new Batch.ReadBatchFactory(stubByShardId, config, clock, metrics));
    }

    static @NonNull Function<Long, Batcher> newWriteBatcherFactory(
            @NonNull ClientConfig config,
            @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
            @NonNull SessionManager sessionManager,
            Clock clock,
            BatchMetrics metrics) {
        return s ->
                new Batcher(
                        config,
                        s,
                        new Batch.WriteBatchFactory(stubByShardId, sessionManager, config, clock, metrics));
    }

    @Override
    public void close() throws Exception {
        operations.add(CloseOperation.INSTANCE);
    }
}
