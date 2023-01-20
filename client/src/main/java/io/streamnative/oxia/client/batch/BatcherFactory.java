package io.streamnative.oxia.client.batch;

public interface BatcherFactory {
    Batcher newWriteBatcher(int shardId);
    Batcher newReadsBatcher(int shardId);
}
