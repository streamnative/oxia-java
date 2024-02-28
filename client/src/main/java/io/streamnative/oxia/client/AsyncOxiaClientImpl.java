/*
 * Copyright © 2022-2024 StreamNative Inc.
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
import io.streamnative.oxia.client.grpc.OxiaStubManager;
import io.streamnative.oxia.client.metrics.BatchMetrics;
import io.streamnative.oxia.client.metrics.OperationMetrics;
import io.streamnative.oxia.client.notify.NotificationManager;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.ListResponse;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class AsyncOxiaClientImpl implements AsyncOxiaClient {

    static @NonNull CompletableFuture<AsyncOxiaClient> newInstance(@NonNull ClientConfig config) {
        var stubManager = new OxiaStubManager();

        var serviceAddrStub = stubManager.getStub(config.serviceAddress());
        var shardManager = new ShardManager(serviceAddrStub, config.metrics(), config.namespace());
        var notificationManager = new NotificationManager(stubManager, shardManager, config.metrics());

        Function<Long, String> leaderFn = shardManager::leader;
        var stubByShardId = leaderFn.andThen(stubManager::getStub);

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
                        stubManager,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager,
                        operationMetrics);

        return shardManager.start().thenApply(v -> client);
    }

    private final @NonNull OxiaStubManager stubManager;
    private final @NonNull ShardManager shardManager;
    private final @NonNull NotificationManager notificationManager;
    private final @NonNull BatchManager readBatchManager;
    private final @NonNull BatchManager writeBatchManager;
    private final @NonNull SessionManager sessionManager;
    private final @NonNull OperationMetrics metrics;
    private final AtomicLong sequence = new AtomicLong();
    private volatile boolean closed;

    @Override
    public @NonNull CompletableFuture<PutResult> put(String key, byte[] value, PutOption... options) {
        var sample = metrics.recordPut(value == null ? 0 : value.length);
        var callback = new CompletableFuture<PutResult>();
        try {
            checkIfClosed();
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);
            var validatedOptions = PutOption.validate(options);
            var shardId = shardManager.get(key);
            var versionId = PutOption.toVersionId(validatedOptions);
            var op =
                    new PutOperation(
                            sequence.getAndIncrement(),
                            callback,
                            key,
                            value,
                            versionId,
                            PutOption.toEphemeral(validatedOptions));
            writeBatchManager.getBatcher(shardId).add(op);
        } catch (RuntimeException e) {
            callback.completeExceptionally(e);
        }
        return callback.whenComplete(sample::stop);
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(String key, DeleteOption... options) {
        var sample = metrics.recordDelete();
        var callback = new CompletableFuture<Boolean>();
        try {
            checkIfClosed();
            Objects.requireNonNull(key);
            var validatedOptions = DeleteOption.validate(options);
            var shardId = shardManager.get(key);
            var versionId = DeleteOption.toVersionId(validatedOptions);
            writeBatchManager
                    .getBatcher(shardId)
                    .add(new DeleteOperation(sequence.getAndIncrement(), callback, key, versionId));
        } catch (RuntimeException e) {
            callback.completeExceptionally(e);
        }
        return callback.whenComplete(sample::stop);
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            String startKeyInclusive, String endKeyExclusive) {
        var sample = metrics.recordDeleteRange();
        CompletableFuture<Void> callback;
        try {
            checkIfClosed();
            Objects.requireNonNull(startKeyInclusive);
            Objects.requireNonNull(endKeyExclusive);
            var shardDeletes =
                    shardManager.getAll().stream()
                            .map(writeBatchManager::getBatcher)
                            .map(
                                    b -> {
                                        var shardCallback = new CompletableFuture<Void>();
                                        b.add(
                                                new DeleteRangeOperation(
                                                        sequence.getAndIncrement(),
                                                        shardCallback,
                                                        startKeyInclusive,
                                                        endKeyExclusive));
                                        return shardCallback;
                                    })
                            .collect(toList())
                            .toArray(new CompletableFuture[0]);
            callback = CompletableFuture.allOf(shardDeletes);
        } catch (RuntimeException e) {
            callback = CompletableFuture.failedFuture(e);
        }
        return callback.whenComplete(sample::stop);
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(String key) {
        var sample = metrics.recordGet();
        var callback = new CompletableFuture<GetResult>();
        try {
            checkIfClosed();
            Objects.requireNonNull(key);
            var shardId = shardManager.get(key);
            readBatchManager
                    .getBatcher(shardId)
                    .add(new GetOperation(sequence.getAndIncrement(), callback, key));
        } catch (RuntimeException e) {
            callback.completeExceptionally(e);
        }
        return callback.whenComplete(sample::stop);
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            String startKeyInclusive, String endKeyExclusive) {
        var sample = metrics.recordList();
        CompletableFuture<List<String>> callback;
        try {
            checkIfClosed();
            Objects.requireNonNull(startKeyInclusive);
            Objects.requireNonNull(endKeyExclusive);
            callback =
                    Flux.fromIterable(shardManager.getAll())
                            .flatMap(shardId -> list(shardId, startKeyInclusive, endKeyExclusive))
                            .collectList()
                            .toFuture();
        } catch (Exception e) {
            callback = CompletableFuture.failedFuture(e);
        }
        return callback.whenComplete(sample::stop);
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        checkIfClosed();
        notificationManager.registerCallback(notificationCallback);
    }

    private @NonNull Flux<String> list(
            long shardId, @NonNull String startKeyInclusive, @NonNull String endKeyExclusive) {
        var leader = shardManager.leader(shardId);
        var stub = stubManager.getStub(leader);
        var request =
                ListRequest.newBuilder()
                        .setShardId(shardId)
                        .setStartInclusive(startKeyInclusive)
                        .setEndExclusive(endKeyExclusive)
                        .build();
        return stub.reactor().list(request).flatMapIterable(ListResponse::getKeysList);
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
        stubManager.close();
    }

    private void checkIfClosed() {
        if (closed) {
            throw new IllegalStateException("Client has been closed");
        }
    }
}
