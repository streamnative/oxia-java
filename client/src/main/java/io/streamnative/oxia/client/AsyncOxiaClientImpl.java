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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
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
import io.streamnative.oxia.client.metrics.Counter;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.client.metrics.LatencyHistogram;
import io.streamnative.oxia.client.metrics.Unit;
import io.streamnative.oxia.client.metrics.UpDownCounter;
import io.streamnative.oxia.client.notify.NotificationManager;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.ListResponse;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;
import reactor.core.publisher.Flux;

class AsyncOxiaClientImpl implements AsyncOxiaClient {

    static @NonNull CompletableFuture<AsyncOxiaClient> newInstance(@NonNull ClientConfig config) {
        var stubManager = new OxiaStubManager();

        var instrumentProvider = new InstrumentProvider(config.openTelemetry(), config.namespace());
        var serviceAddrStub = stubManager.getStub(config.serviceAddress());
        var shardManager = new ShardManager(serviceAddrStub, instrumentProvider, config.namespace());
        var notificationManager =
                new NotificationManager(stubManager, shardManager, instrumentProvider);

        Function<Long, String> leaderFn = shardManager::leader;
        var stubByShardId = leaderFn.andThen(stubManager::getStub);

        shardManager.addCallback(notificationManager);
        var readBatchManager =
                BatchManager.newReadBatchManager(config, stubByShardId, instrumentProvider);
        var sessionManager = new SessionManager(config, stubByShardId, instrumentProvider);
        shardManager.addCallback(sessionManager);
        var writeBatchManager =
                BatchManager.newWriteBatchManager(
                        config, stubByShardId, sessionManager, instrumentProvider);

        var client =
                new AsyncOxiaClientImpl(
                        instrumentProvider,
                        stubManager,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager);

        return shardManager.start().thenApply(v -> client);
    }

    private final @NonNull InstrumentProvider instrumentProvider;
    private final @NonNull OxiaStubManager stubManager;
    private final @NonNull ShardManager shardManager;
    private final @NonNull NotificationManager notificationManager;
    private final @NonNull BatchManager readBatchManager;
    private final @NonNull BatchManager writeBatchManager;
    private final @NonNull SessionManager sessionManager;
    private final AtomicLong sequence = new AtomicLong();
    private volatile boolean closed;

    private final Counter counterPutBytes;
    private final Counter counterGetBytes;
    private final Counter counterListBytes;

    private final UpDownCounter gaugePendingPutRequests;
    private final UpDownCounter gaugePendingGetRequests;
    private final UpDownCounter gaugePendingListRequests;
    private final UpDownCounter gaugePendingDeleteRequests;
    private final UpDownCounter gaugePendingDeleteRangeRequests;

    private final UpDownCounter gaugePendingPutBytes;

    private final LatencyHistogram histogramPutLatency;
    private final LatencyHistogram histogramGetLatency;
    private final LatencyHistogram histogramDeleteLatency;
    private final LatencyHistogram histogramDeleteRangeLatency;
    private final LatencyHistogram histogramListLatency;

    AsyncOxiaClientImpl(
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull OxiaStubManager stubManager,
            @NonNull ShardManager shardManager,
            @NonNull NotificationManager notificationManager,
            @NonNull BatchManager readBatchManager,
            @NonNull BatchManager writeBatchManager,
            @NonNull SessionManager sessionManager) {
        this.instrumentProvider = instrumentProvider;
        this.stubManager = stubManager;
        this.shardManager = shardManager;
        this.notificationManager = notificationManager;
        this.readBatchManager = readBatchManager;
        this.writeBatchManager = writeBatchManager;
        this.sessionManager = sessionManager;

        counterPutBytes =
                instrumentProvider.newCounter(
                        "oxia.client.ops.size",
                        Unit.Bytes,
                        "Total number of bytes in operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "put"));
        counterGetBytes =
                instrumentProvider.newCounter(
                        "oxia.client.ops.size",
                        Unit.Bytes,
                        "Total number of bytes in operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "get"));
        counterListBytes =
                instrumentProvider.newCounter(
                        "oxia.client.list.size",
                        Unit.Bytes,
                        "Total number of bytes read in list operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "list"));

        gaugePendingPutRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "put"));
        gaugePendingGetRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "get"));
        gaugePendingListRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "list"));
        gaugePendingDeleteRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "delete"));
        gaugePendingDeleteRangeRequests =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.pending",
                        Unit.Events,
                        "Current number of outstanding requests",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "delete-range"));

        gaugePendingPutBytes =
                instrumentProvider.newUpDownCounter(
                        "oxia.client.ops.outstanding",
                        Unit.Bytes,
                        "Current number of outstanding bytes in put operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "put"));

        histogramPutLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "put"));

        histogramGetLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "get"));

        histogramDeleteLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "delete"));

        histogramDeleteRangeLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "delete-range"));

        histogramListLatency =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops",
                        "Duration of operations",
                        Attributes.of(AttributeKey.stringKey("oxia.op"), "list"));
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(String key, byte[] value, PutOption... options) {
        long startTime = System.nanoTime();
        var callback = new CompletableFuture<PutResult>();

        try {
            checkIfClosed();
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);

            gaugePendingPutRequests.increment();
            gaugePendingPutBytes.add(value.length);

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
        return callback.whenComplete(
                (putResult, throwable) -> {
                    gaugePendingPutRequests.decrement();
                    gaugePendingPutBytes.add(-value.length);

                    if (throwable == null) {
                        counterPutBytes.add(value.length);
                        histogramPutLatency.recordSuccess(System.nanoTime() - startTime);
                    } else {
                        histogramPutLatency.recordFailure(System.nanoTime() - startTime);
                    }
                });
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(String key, DeleteOption... options) {
        long startTime = System.nanoTime();

        gaugePendingDeleteRequests.increment();

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
        return callback.whenComplete(
                (putResult, throwable) -> {
                    gaugePendingDeleteRequests.decrement();
                    if (throwable == null) {
                        histogramDeleteLatency.recordSuccess(System.nanoTime() - startTime);
                    } else {
                        histogramDeleteLatency.recordFailure(System.nanoTime() - startTime);
                    }
                });
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            String startKeyInclusive, String endKeyExclusive) {
        long startTime = System.nanoTime();
        gaugePendingDeleteRangeRequests.increment();
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
        return callback.whenComplete(
                (putResult, throwable) -> {
                    gaugePendingDeleteRequests.decrement();
                    if (throwable == null) {
                        histogramDeleteRangeLatency.recordSuccess(System.nanoTime() - startTime);
                    } else {
                        histogramDeleteRangeLatency.recordFailure(System.nanoTime() - startTime);
                    }
                });
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(String key) {
        long startTime = System.nanoTime();
        gaugePendingGetRequests.increment();
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
        return callback.whenComplete(
                (getResult, throwable) -> {
                    gaugePendingGetRequests.decrement();
                    if (throwable == null) {
                        if (getResult != null) {
                            counterGetBytes.add(getResult.getValue().length);
                        }
                        histogramGetLatency.recordSuccess(System.nanoTime() - startTime);
                    } else {
                        histogramGetLatency.recordFailure(System.nanoTime() - startTime);
                    }
                });
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            String startKeyInclusive, String endKeyExclusive) {
        long startTime = System.nanoTime();
        gaugePendingListRequests.increment();
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
        return callback.whenComplete(
                (listResult, throwable) -> {
                    gaugePendingListRequests.decrement();
                    if (throwable == null) {
                        counterListBytes.add(listResult.stream().mapToInt(String::length).sum());
                        histogramListLatency.recordSuccess(System.nanoTime() - startTime);
                    } else {
                        histogramListLatency.recordFailure(System.nanoTime() - startTime);
                    }
                });
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
