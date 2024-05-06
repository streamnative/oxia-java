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

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStubProvider;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.client.session.SessionManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import lombok.NonNull;
import lombok.SneakyThrows;

public class Batcher implements AutoCloseable {

    private static final int DEFAULT_INITIAL_QUEUE_CAPACITY = 1_000;

    @NonNull private final ClientConfig config;
    private final long shardId;
    @NonNull private final BatchFactory batchFactory;
    @NonNull private final BlockingQueue<Operation<?>> operations;

    private final Thread thread;

    Batcher(@NonNull ClientConfig config, long shardId, @NonNull BatchFactory batchFactory) {
        this(config, shardId, batchFactory, new ArrayBlockingQueue<>(DEFAULT_INITIAL_QUEUE_CAPACITY));
    }

    Batcher(
            @NonNull ClientConfig config,
            long shardId,
            @NonNull BatchFactory batchFactory,
            @NonNull BlockingQueue<Operation<?>> operations) {
        this.config = config;
        this.shardId = shardId;
        this.batchFactory = batchFactory;
        this.operations = operations;

        this.thread =
                new DefaultThreadFactory(String.format("batcher-shard-%d", shardId))
                        .newThread(this::batcherLoop);
        this.thread.start();
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

    public void batcherLoop() {
        Batch batch = null;
        long lingerBudgetNanos = -1L;
        while (true) {
            Operation<?> operation;

            try {
                if (batch == null) {
                    operation = operations.take();
                } else {
                    operation = operations.poll(lingerBudgetNanos, NANOSECONDS);
                    long spentLingerBudgetNanos = Math.max(0, System.nanoTime() - batch.getStartTimeNanos());
                    lingerBudgetNanos = Math.max(0L, lingerBudgetNanos - spentLingerBudgetNanos);
                }
            } catch (InterruptedException e) {
                // Exiting thread
                return;
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
        }
    }

    static @NonNull Function<Long, Batcher> newReadBatcherFactory(
            @NonNull ClientConfig config,
            @NonNull OxiaStubProvider stubProvider,
            InstrumentProvider instrumentProvider) {
        return s ->
                new Batcher(config, s, new ReadBatchFactory(stubProvider, config, instrumentProvider));
    }

    static @NonNull Function<Long, Batcher> newWriteBatcherFactory(
            @NonNull ClientConfig config,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull SessionManager sessionManager,
            InstrumentProvider instrumentProvider) {
        return s ->
                new Batcher(
                        config,
                        s,
                        new WriteBatchFactory(stubProvider, sessionManager, config, instrumentProvider));
    }

    @Override
    public void close() {
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
