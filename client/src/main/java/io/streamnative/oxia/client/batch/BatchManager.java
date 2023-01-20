package io.streamnative.oxia.client.batch;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BatchManager implements AutoCloseable {

    private final ConcurrentMap<Integer, Batcher> batchersByShardId = new ConcurrentHashMap<>();
    private BatcherFactory batcherFactory = null;

    Batcher getBatcher(int shardId) {
        return batchersByShardId.computeIfAbsent(shardId, s -> batcherFactory.newBatcher(s));
    }

    @Override
    public void close() throws Exception {

    }
}
