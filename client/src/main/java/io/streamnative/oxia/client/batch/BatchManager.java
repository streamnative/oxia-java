package io.streamnative.oxia.client.batch;

import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PACKAGE;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchManager implements AutoCloseable {

    private final ConcurrentMap<Long, Batcher> batchersByShardId = new ConcurrentHashMap<>();
    private final Function<Long, Batcher> batcherFactory;
    private volatile boolean closed;

    Batcher getBatcher(long shardId) {
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
        @Getter private final List<Exception> exceptions;
    }
}
