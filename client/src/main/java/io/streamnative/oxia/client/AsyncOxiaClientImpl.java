package io.streamnative.oxia.client;

import static java.util.stream.Collectors.toList;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.batch.BatchManager;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.shard.ShardManager;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class AsyncOxiaClientImpl implements AsyncOxiaClient {
    private final ShardManager shardManager;
    private final BatchManager readBatchManager;
    private final BatchManager writeBatchManager;

    AsyncOxiaClientImpl(ClientConfig config) {
        shardManager = new ShardManager(config.serviceAddress(), null);
        readBatchManager = BatchManager.newReadBatchManager(config, null);
        writeBatchManager = BatchManager.newWriteBatchManager(config, null);
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(
            @NonNull String key, byte @NonNull [] payload, long expectedVersionId) {
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<PutResult>();
        writeBatchManager
                .getBatcher(shardId)
                .add(new PutOperation(callback, key, payload, expectedVersionId));
        return callback;
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(@NonNull String key, byte @NonNull [] payload) {
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<PutResult>();
        writeBatchManager.getBatcher(shardId).add(new PutOperation(callback, key, payload));
        return callback;
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(@NonNull String key, long expectedVersionId) {
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<Boolean>();
        writeBatchManager
                .getBatcher(shardId)
                .add(new DeleteOperation(callback, key, expectedVersionId));
        return callback;
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(@NonNull String key) {
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<Boolean>();
        writeBatchManager.getBatcher(shardId).add(new DeleteOperation(callback, key));
        return callback;
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        return CompletableFuture.allOf(
                shardManager.getAll().stream()
                        .map(writeBatchManager::getBatcher)
                        .map(
                                b -> {
                                    var callback = new CompletableFuture<Void>();
                                    b.add(new DeleteRangeOperation(callback, minKeyInclusive, maxKeyExclusive));
                                    return callback;
                                })
                        .collect(toList())
                        .toArray(new CompletableFuture[0]));
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(@NonNull String key) {
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<GetResult>();
        readBatchManager.getBatcher(shardId).add(new GetOperation(callback, key));
        return callback;
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        List<CompletableFuture<List<String>>> responses =
                shardManager.getAll().stream()
                        .map(readBatchManager::getBatcher)
                        .map(
                                b -> {
                                    var callback = new CompletableFuture<List<String>>();
                                    b.add(new ListOperation(callback, minKeyInclusive, maxKeyExclusive));
                                    return callback;
                                })
                        .collect(toList());
        return CompletableFuture.allOf(responses.toArray(new CompletableFuture[0]))
                .thenApply(
                        v ->
                                responses.stream()
                                        .map(
                                                r -> {
                                                    try {
                                                        return r.get();
                                                    } catch (InterruptedException e) {
                                                        Thread.currentThread().interrupt();
                                                        throw new RuntimeException(e);
                                                    } catch (ExecutionException e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                })
                                        .flatMap(Collection::stream)
                                        .collect(toList()));
    }

    @Override
    public void close() throws Exception {
        readBatchManager.close();
        writeBatchManager.close();
        shardManager.close();
    }
}
