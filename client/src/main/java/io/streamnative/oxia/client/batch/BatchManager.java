package io.streamnative.oxia.client.batch;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BatchManager implements AutoCloseable {

    private final ConcurrentMap<Long, Batcher> batchersByShardId = new ConcurrentHashMap<>();
    private final Function<Long, Batcher> batcherFactory;

    Batcher getBatcher(long shardId) {
        return batchersByShardId.computeIfAbsent(shardId, batcherFactory);
    }

    @Override
    public void close() throws Exception {}
}
