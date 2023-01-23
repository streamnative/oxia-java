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

    //    public AsyncOxiaClientImpl(ClientConfig config) {
    //            this(
    //                    new ShardManager(),
    //                    new BatchManager(),
    //                    new BatchManager()
    //            );
    //    );

    @Override
    public @NonNull CompletableFuture<PutResult> put(
            @NonNull String key, byte @NonNull [] payload, long expectedVersion) {
        var shardId = shardManager.get(key);
        return writeBatchManager
                .getBatcher(shardId)
                .add(new PutOperation(key, payload, expectedVersion));
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(@NonNull String key, byte @NonNull [] payload) {
        var shardId = shardManager.get(key);
        return writeBatchManager.getBatcher(shardId).add(new PutOperation(key, payload));
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(@NonNull String key, long expectedVersion) {
        var shardId = shardManager.get(key);
        return writeBatchManager.getBatcher(shardId).add(new DeleteOperation(key, expectedVersion));
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(@NonNull String key) {
        var shardId = shardManager.get(key);
        return writeBatchManager.getBatcher(shardId).add(new DeleteOperation(key));
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        return CompletableFuture.allOf(
                shardManager.getAll().stream()
                        .map(writeBatchManager::getBatcher)
                        .map(b -> b.add(new DeleteRangeOperation(minKeyInclusive, maxKeyExclusive)))
                        .collect(toList())
                        .toArray(new CompletableFuture[0]));
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(@NonNull String key) {
        var shardId = shardManager.get(key);
        return readBatchManager.getBatcher(shardId).add(new GetOperation(key));
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        List<CompletableFuture<List<String>>> responses =
                shardManager.getAll().stream()
                        .map(readBatchManager::getBatcher)
                        .map(b -> b.add(new ListOperation(minKeyInclusive, maxKeyExclusive)))
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
