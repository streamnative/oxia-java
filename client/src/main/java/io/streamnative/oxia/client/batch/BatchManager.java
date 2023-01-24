package io.streamnative.oxia.client.batch;

import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
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
        return new BatchManager(Batcher.newReadBatcherFactory(config, clientByShardId));
    }

    public static @NonNull BatchManager newWriteBatchManager(
            @NonNull ClientConfig config,
            @NonNull Function<Long, Optional<OxiaClientBlockingStub>> clientByShardId) {
        return new BatchManager(Batcher.newReadBatcherFactory(config, clientByShardId));
    }
}
