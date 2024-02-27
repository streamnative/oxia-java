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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.BatchMetrics;
import io.streamnative.oxia.client.session.SessionManager;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchManager implements AutoCloseable {

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ConcurrentMap<Long, Batcher> batchersByShardId = new ConcurrentHashMap<>();
    private final @NonNull Function<Long, Batcher> batcherFactory;
    private volatile boolean closed;

    public Batcher getBatcher(long shardId) {
        if (closed) {
            throw new IllegalStateException("Batch manager is closed");
        }
        return batchersByShardId.computeIfAbsent(shardId, this::createAndStartBatcher);
    }

    private Batcher createAndStartBatcher(long shardId) {
        Batcher batcher = batcherFactory.apply(shardId);
        executor.execute(batcher);
        return batcher;
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        var exceptions =
                batchersByShardId.values().stream()
                        .map(
                                b -> {
                                    try {
                                        b.close();
                                        return null;
                                    } catch (Exception e) {
                                        return e;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(toList());
        shutdownExecutor();
        if (!exceptions.isEmpty()) {
            throw new ShutdownException(exceptions);
        }
    }

    private void shutdownExecutor() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    @RequiredArgsConstructor(access = PACKAGE)
    public static class ShutdownException extends Exception {
        @Getter private final @NonNull List<Exception> exceptions;
    }

    public static @NonNull BatchManager newReadBatchManager(
            @NonNull ClientConfig config,
            @NonNull Function<Long, OxiaStub> stubByShardId,
            BatchMetrics metrics) {
        return new BatchManager(
                Batcher.newReadBatcherFactory(config, stubByShardId, Clock.systemUTC(), metrics));
    }

    public static @NonNull BatchManager newWriteBatchManager(
            @NonNull ClientConfig config,
            @NonNull Function<Long, OxiaStub> stubByShardId,
            @NonNull SessionManager sessionManager,
            BatchMetrics metrics) {
        return new BatchManager(
                Batcher.newWriteBatcherFactory(
                        config, stubByShardId, sessionManager, Clock.systemUTC(), metrics));
    }
}
