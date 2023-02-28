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
import io.streamnative.oxia.client.metrics.BatchMetrics;
import io.streamnative.oxia.client.metrics.OperationMetrics;
import io.streamnative.oxia.client.notify.NotificationManager;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.ListResponse;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.time.Clock;
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

    static @NonNull CompletableFuture<AsyncOxiaClient> newInstance(@NonNull ClientConfig config) {
        var channelManager = new ChannelManager();
        var reactorStubFactory = channelManager.getReactorStubFactory();
        Supplier<ReactorOxiaClientStub> stubFactory =
                () -> reactorStubFactory.apply(config.serviceAddress());
        var shardManager = new ShardManager(stubFactory);
        var notificationManager = new NotificationManager(reactorStubFactory);

        Function<Long, String> leaderFn = shardManager::leader;
        var stubByShardId = leaderFn.andThen(reactorStubFactory);
        shardManager.addCallback(notificationManager);
        var batchMetrics = BatchMetrics.create(Clock.systemUTC(), config.metrics());
        var readBatchManager = BatchManager.newReadBatchManager(config, stubByShardId, batchMetrics);
        var sessionManager = new SessionManager(config, stubByShardId);
        shardManager.addCallback(sessionManager);
        var writeBatchManager =
                BatchManager.newWriteBatchManager(config, stubByShardId, sessionManager, batchMetrics);
        var operationMetrics = OperationMetrics.create(Clock.systemUTC(), config.metrics());

        var client =
                new AsyncOxiaClientImpl(
                        channelManager,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager,
                        reactorStubFactory,
                        operationMetrics);

        return shardManager.start().thenApply(v -> client);
    }

    private final @NonNull ChannelManager channelManager;
    private final @NonNull ShardManager shardManager;
    private final @NonNull NotificationManager notificationManager;
    private final @NonNull BatchManager readBatchManager;
    private final @NonNull BatchManager writeBatchManager;
    private final @NonNull SessionManager sessionManager;
    private final @NonNull StubFactory<ReactorOxiaClientStub> reactorStubFactory;
    private final @NonNull OperationMetrics metrics;
    private volatile boolean closed;

    @Override
    public @NonNull CompletableFuture<PutResult> put(
            @NonNull String key, byte @NonNull [] value, @NonNull PutOption... options) {
        checkIfClosed();
        var sample = metrics.recordPut(value.length);
        var validatedOptions = PutOption.validate(options);
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<PutResult>();
        var versionId = PutOption.toVersionId(validatedOptions);
        var op =
                new PutOperation(callback, key, value, versionId, PutOption.toEphemeral(validatedOptions));
        writeBatchManager.getBatcher(shardId).add(op);
        return callback.whenComplete(sample);
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(
            @NonNull String key, @NonNull DeleteOption... options) {
        checkIfClosed();
        var sample = metrics.recordDelete();
        var validatedOptions = DeleteOption.validate(options);
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<Boolean>();
        var versionId = DeleteOption.toVersionId(validatedOptions);
        writeBatchManager.getBatcher(shardId).add(new DeleteOperation(callback, key, versionId));
        return callback.whenComplete(sample);
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        checkIfClosed();
        var sample = metrics.recordDeleteRange();
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
                                .toArray(new CompletableFuture[0]))
                .whenComplete(sample);
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(@NonNull String key) {
        checkIfClosed();
        var sample = metrics.recordGet();
        var shardId = shardManager.get(key);
        var callback = new CompletableFuture<GetResult>();
        readBatchManager.getBatcher(shardId).add(new GetOperation(callback, key));
        return callback.whenComplete(sample);
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        checkIfClosed();
        var sample = metrics.recordList();
        return Flux.fromIterable(shardManager.getAll())
                .flatMap(shardId -> list(shardId, minKeyInclusive, maxKeyExclusive))
                .collectList()
                .toFuture()
                .whenComplete(sample);
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        checkIfClosed();
        notificationManager.registerCallback(notificationCallback);
    }

    private @NonNull Flux<String> list(
            long shardId, @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        checkIfClosed();
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
        if (closed) {
            return;
        }
        closed = true;
        readBatchManager.close();
        writeBatchManager.close();
        sessionManager.close();
        notificationManager.close();
        shardManager.close();
        channelManager.close();
    }

    private void checkIfClosed() {
        if (closed) {
            throw new IllegalStateException("Client has been closed");
        }
    }
}
