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
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.batch.BatchManager;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.grpc.ChannelManager;
import io.streamnative.oxia.client.grpc.ChannelManager.StubFactory;
import io.streamnative.oxia.client.notify.NotificationManager;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.ListResponse;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class AsyncOxiaClientImpl implements AsyncOxiaClient {

    static CompletableFuture<AsyncOxiaClient> newInstance(ClientConfig config) {
        var channelManager = new ChannelManager();
        var reactorStubFactory = channelManager.getReactorStubFactory();
        Supplier<ReactorOxiaClientStub> stubFactory =
                () -> reactorStubFactory.apply(config.serviceAddress());
        var shardManager = new ShardManager(stubFactory);
        var notificationManager = new NotificationManager(reactorStubFactory);

        Function<Long, String> leaderFn = shardManager::leader;
        var stubByShardId = leaderFn.andThen(reactorStubFactory);
        shardManager.addCallback(notificationManager);
        var readBatchManager = BatchManager.newReadBatchManager(config, stubByShardId);
        var sessionManager = new SessionManager(config, stubByShardId);
        var writeBatchManager =
                BatchManager.newWriteBatchManager(config, stubByShardId, sessionManager);

        var client =
                new AsyncOxiaClientImpl(
                        channelManager,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager,
                        reactorStubFactory);

        return shardManager.start().thenApply(v -> client);
    }

    private final ChannelManager channelManager;
    private final ShardManager shardManager;
    private final NotificationManager notificationManager;
    private final BatchManager readBatchManager;
    private final BatchManager writeBatchManager;
    private final SessionManager sessionManager;
    private final StubFactory<ReactorOxiaClientStub> reactorStubFactory;

    @Override
    public @NonNull CompletableFuture<PutResult> put(
            @NonNull String key, byte @NonNull [] value, @NonNull PutOption... options) {
        var validatedOptions = PutOption.validate(options);
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<PutResult>();
        var versionId = PutOption.toVersionId(validatedOptions);
        var op =
                new PutOperation(callback, key, value, versionId, PutOption.toEphemeral(validatedOptions));
        writeBatchManager.getBatcher(shardId).add(op);
        return callback;
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(@NonNull String key, DeleteOption... options) {
        var validatedOptions = DeleteOption.validate(options);
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<Boolean>();
        var versionId = DeleteOption.toVersionId(validatedOptions);
        writeBatchManager.getBatcher(shardId).add(new DeleteOperation(callback, key, versionId));
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
        return Flux.fromIterable(shardManager.getAll())
                .flatMap(shardId -> list(shardId, minKeyInclusive, maxKeyExclusive))
                .collectList()
                .toFuture();
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        notificationManager.registerCallback(notificationCallback);
    }

    private Flux<String> list(
            long shardId, @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        var leader = shardManager.leader(shardId);
        var stub = reactorStubFactory.apply(leader);
        var request =
                ListRequest.newBuilder()
                        .setShardId(ProtoUtil.longToUint32(shardId))
                        .setStartInclusive(minKeyInclusive)
                        .setEndExclusive(maxKeyExclusive)
                        .build();
        return stub.list(request).flatMapIterable(ListResponse::getKeysList);
    }

    @Override
    public void close() throws Exception {
        readBatchManager.close();
        writeBatchManager.close();
        sessionManager.close();
        notificationManager.close();
        shardManager.close();
        channelManager.close();
    }
}
