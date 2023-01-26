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
package io.streamnative.oxia.client.batch;

import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchManager implements AutoCloseable {

    private final ConcurrentMap<Long, Batcher> batchersByShardId = new ConcurrentHashMap<>();
    private final @NonNull Function<Long, Batcher> batcherFactory;
    private volatile boolean closed;

    public Batcher getBatcher(long shardId) {
        if (closed) {
            throw new IllegalStateException("Batch manager is closed");
        }
        return batchersByShardId.computeIfAbsent(shardId, batcherFactory);
    }

    @Override
    public void close() throws Exception {
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
        if (!exceptions.isEmpty()) {
            throw new ShutdownException(exceptions);
        }
    }

    @RequiredArgsConstructor(access = PACKAGE)
    public static class ShutdownException extends Exception {
        @Getter private final @NonNull List<Exception> exceptions;
    }

    public static @NonNull BatchManager newReadBatchManager(
            @NonNull ClientConfig config,
            @NonNull Function<Long, Optional<OxiaClientBlockingStub>> clientByShardId) {
        return new BatchManager(
                Batcher.newReadBatcherFactory(config, clientByShardId, Clock.systemUTC()));
    }

    public static @NonNull BatchManager newWriteBatchManager(
            @NonNull ClientConfig config,
            @NonNull Function<Long, Optional<OxiaClientBlockingStub>> clientByShardId) {
        return new BatchManager(
                Batcher.newReadBatcherFactory(config, clientByShardId, Clock.systemUTC()));
    }
}
