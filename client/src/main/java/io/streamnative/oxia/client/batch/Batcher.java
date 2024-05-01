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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.batch.Operation.CloseOperation;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.client.session.SessionManager;
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
    @NonNull private final BatchFactory batchFactory;
    @NonNull private final BlockingQueue<Operation<?>> operations;

    Batcher(@NonNull ClientConfig config, long shardId, @NonNull BatchFactory batchFactory) {
        this(
                config,
                shardId,
                batchFactory,
                new PriorityBlockingQueue<>(DEFAULT_INITIAL_QUEUE_CAPACITY, Operation.PriorityComparator));
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
        long lingerBudgetNanos = -1L;
        while (true) {
            try {
                Operation<?> operation;
                if (batch == null) {
                    operation = operations.take();
                } else {
                    operation = operations.poll(lingerBudgetNanos, NANOSECONDS);
                    long spentLingerBudgetNanos = Math.max(0, System.nanoTime() - batch.getStartTimeNanos());
                    lingerBudgetNanos = Math.max(0L, lingerBudgetNanos - spentLingerBudgetNanos);
                }

                if (operation == CloseOperation.INSTANCE) {
                    break;
                }

                if (operation != null) {
                    if (batch == null) {
                        batch = batchFactory.getBatch(shardId);
                        lingerBudgetNanos = config.batchLinger().toNanos();
                    }
                    try {
                        if (!batch.canAdd(operation)) {
                            batch.send();
                            batch = batchFactory.getBatch(shardId);
                            lingerBudgetNanos = config.batchLinger().toNanos();
                        }
                        batch.add(operation);
                    } catch (Exception e) {
                        operation.fail(e);
                    }
                }

                if (batch != null) {
                    if (batch.size() == config.maxRequestsPerBatch() || lingerBudgetNanos == 0) {
                        batch.send();
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
            @NonNull Function<Long, OxiaStub> stubByShardId,
            InstrumentProvider instrumentProvider) {
        return s ->
                new Batcher(config, s, new ReadBatchFactory(stubByShardId, config, instrumentProvider));
    }

    static @NonNull Function<Long, Batcher> newWriteBatcherFactory(
            @NonNull ClientConfig config,
            @NonNull Function<Long, OxiaStub> stubByShardId,
            @NonNull SessionManager sessionManager,
            InstrumentProvider instrumentProvider) {
        return s ->
                new Batcher(
                        config,
                        s,
                        new WriteBatchFactory(stubByShardId, sessionManager, config, instrumentProvider));
    }

    @Override
    public void close() {
        operations.add(CloseOperation.INSTANCE);
    }
}
