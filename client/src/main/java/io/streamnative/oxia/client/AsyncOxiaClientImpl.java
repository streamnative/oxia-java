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
package io.streamnative.oxia.client;

import static java.util.stream.Collectors.toList;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutOptions;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.batch.BatchManager;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.grpc.ChannelManager;
import io.streamnative.oxia.client.notify.NotificationManager;
import io.streamnative.oxia.client.notify.NotificationManagerImpl;
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

    static CompletableFuture<AsyncOxiaClient> newInstance(ClientConfig config) {
        var channelManager = new ChannelManager();
        var shardManager = new ShardManager(config.serviceAddress(), channelManager.getStubFactory());
        var notificationManager =
                config.notificationCallback() == null
                        ? NotificationManagerImpl.NullObject
                        : new NotificationManagerImpl(
                                config.serviceAddress(),
                                channelManager.getStubFactory(),
                                config.notificationCallback());

        var blockingStubByShardId =
                new BlockingStubByShardId(shardManager, channelManager.getBlockingStubFactory(), config);
        var readBatchManager = BatchManager.newReadBatchManager(config, blockingStubByShardId);
        var writeBatchManager = BatchManager.newWriteBatchManager(config, blockingStubByShardId);

        var client =
                new AsyncOxiaClientImpl(
                        channelManager, shardManager, notificationManager, readBatchManager, writeBatchManager);

        return CompletableFuture.allOf(shardManager.start(), notificationManager.start())
                .thenApply(v -> client);
    }

    private final ChannelManager channelManager;
    private final ShardManager shardManager;
    private final NotificationManager notificationManager;
    private final BatchManager readBatchManager;
    private final BatchManager writeBatchManager;

    @Override
    public @NonNull CompletableFuture<PutResult> put(
            @NonNull String key, byte @NonNull [] value, @NonNull PutOptions options) {
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<PutResult>();
        writeBatchManager
                .getBatcher(shardId)
                .add(new PutOperation(callback, key, value, options.expectedVersionId()));
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
        notificationManager.close();
        shardManager.close();
        channelManager.close();
    }
}
