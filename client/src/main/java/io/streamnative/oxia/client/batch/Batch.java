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
package io.streamnative.oxia.client.batch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation.SessionInfo;
import io.streamnative.oxia.client.metrics.BatchMetrics;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import io.streamnative.oxia.proto.ReadRequest;
import io.streamnative.oxia.proto.WriteRequest;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

public interface Batch {

    long getStartTime();

    void add(@NonNull Operation<?> operation);

    boolean canAdd(@NonNull Operation<?> operation);

    int size();

    long getShardId();

    void complete();

    final class WriteBatch extends BatchBase implements Batch {
        @VisibleForTesting final List<PutOperation> puts = new ArrayList<>();
        @VisibleForTesting final List<DeleteOperation> deletes = new ArrayList<>();
        @VisibleForTesting final List<DeleteRangeOperation> deleteRanges = new ArrayList<>();
        private final SessionManager sessionManager;
        private final String clientIdentifier;
        private final int maxBatchSize;
        private boolean containsEphemeral;
        private int byteSize;
        private long bytes;

        WriteBatch(
                @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
                @NonNull SessionManager sessionManager,
                @NonNull String clientIdentifier,
                long shardId,
                long createTime,
                int maxBatchSize,
                BatchMetrics.Sample sample) {
            super(stubByShardId, shardId, createTime, sample);
            this.sessionManager = sessionManager;
            this.clientIdentifier = clientIdentifier;
            this.byteSize = 0;
            this.maxBatchSize = maxBatchSize;
        }

        int sizeOf(@NonNull Operation<?> operation) {
            if (operation instanceof PutOperation p) {
                return p.key().getBytes(UTF_8).length + p.value().length;
            } else if (operation instanceof DeleteOperation d) {
                return d.key().getBytes(UTF_8).length;
            } else if (operation instanceof DeleteRangeOperation r) {
                return r.startKeyInclusive().getBytes(UTF_8).length
                        + r.endKeyExclusive().getBytes(UTF_8).length;
            }
            return 0;
        }

        public void add(@NonNull Operation<?> operation) {
            if (operation instanceof PutOperation p) {
                puts.add(p);
                bytes += p.value().length;
                containsEphemeral |= p.ephemeral();
            } else if (operation instanceof DeleteOperation d) {
                deletes.add(d);
            } else if (operation instanceof DeleteRangeOperation r) {
                deleteRanges.add(r);
            }
            byteSize += sizeOf(operation);
        }

        @Override
        public boolean canAdd(@NonNull Operation<?> operation) {
            int size = sizeOf(operation);
            return byteSize + size <= maxBatchSize;
        }

        @Override
        public int size() {
            return puts.size() + deletes.size() + deleteRanges.size();
        }

        @Override
        public void complete() {
            sample.startExec();
            Throwable t = null;
            try {
                var response = getStub().write(toProto()).block();
                for (var i = 0; i < deletes.size(); i++) {
                    deletes.get(i).complete(response.getDeletes(i));
                }
                for (var i = 0; i < deleteRanges.size(); i++) {
                    deleteRanges.get(i).complete(response.getDeleteRanges(i));
                }
                for (var i = 0; i < puts.size(); i++) {
                    puts.get(i).complete(response.getPuts(i));
                }
            } catch (Throwable batchError) {
                t = batchError;
                deletes.forEach(d -> d.fail(batchError));
                deleteRanges.forEach(f -> f.fail(batchError));
                puts.forEach(p -> p.fail(batchError));
            }
            sample.stop(t, bytes, size());
        }

        @NonNull
        WriteRequest toProto() {
            Optional<SessionInfo> sessionInfo;
            if (containsEphemeral) {
                sessionInfo =
                        Optional.of(
                                new SessionInfo(
                                        sessionManager.getSession(getShardId()).getSessionId(), clientIdentifier));
            } else {
                sessionInfo = Optional.empty();
            }
            return WriteRequest.newBuilder()
                    .setShardId(getShardId())
                    .addAllPuts(puts.stream().map(p -> p.toProto(sessionInfo)).collect(toList()))
                    .addAllDeletes(deletes.stream().map(DeleteOperation::toProto).collect(toList()))
                    .addAllDeleteRanges(
                            deleteRanges.stream().map(DeleteRangeOperation::toProto).collect(toList()))
                    .build();
        }
    }

    final class ReadBatch extends BatchBase implements Batch {
        @VisibleForTesting final List<GetOperation> gets = new ArrayList<>();

        @Override
        public boolean canAdd(@NonNull Operation<?> operation) {
            return true;
        }

        public void add(@NonNull Operation<?> operation) {
            if (operation instanceof GetOperation g) {
                gets.add(g);
            }
        }

        ReadBatch(
                @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
                long shardId,
                long createTime,
                BatchMetrics.Sample sample) {
            super(stubByShardId, shardId, createTime, sample);
        }

        @Override
        public int size() {
            return gets.size();
        }

        @Override
        public void complete() {
            sample.startExec();
            Throwable t = null;
            LongAdder bytes = new LongAdder();
            try {
                var responses =
                        getStub()
                                .read(toProto())
                                .flatMapSequential(response -> Flux.fromIterable(response.getGetsList()))
                                .doOnNext(r -> bytes.add(r.getValue().size()));
                Flux.fromIterable(gets).zipWith(responses, this::complete).then().block();
            } catch (Throwable batchError) {
                t = batchError;
                gets.forEach(g -> g.fail(batchError));
            }
            sample.stop(t, bytes.sum(), size());
        }

        private boolean complete(GetOperation operation, GetResponse response) {
            operation.complete(response);
            return true;
        }

        @NonNull
        ReadRequest toProto() {
            return ReadRequest.newBuilder()
                    .setShardId(getShardId())
                    .addAllGets(gets.stream().map(GetOperation::toProto).collect(toList()))
                    .build();
        }
    }

    @RequiredArgsConstructor(access = PRIVATE)
    abstract class BatchBase {
        private final @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId;
        @Getter private final long shardId;
        @Getter private final long startTime;
        final BatchMetrics.Sample sample;

        protected ReactorOxiaClientStub getStub() {
            return stubByShardId.apply(shardId);
        }
    }

    @RequiredArgsConstructor(access = PACKAGE)
    abstract class BatchFactory implements Function<Long, Batch> {
        final @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId;

        @Getter(PACKAGE)
        private final @NonNull ClientConfig config;

        final @NonNull Clock clock;
        final @NonNull BatchMetrics metrics;

        public abstract @NonNull Batch apply(@NonNull Long shardId);
    }

    class WriteBatchFactory extends BatchFactory {
        final @NonNull SessionManager sessionManager;

        public WriteBatchFactory(
                @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
                @NonNull SessionManager sessionManager,
                @NonNull ClientConfig config,
                @NonNull Clock clock,
                @NonNull BatchMetrics metrics) {
            super(stubByShardId, config, clock, metrics);
            this.sessionManager = sessionManager;
        }

        @Override
        public @NonNull Batch apply(@NonNull Long shardId) {
            return new WriteBatch(
                    stubByShardId,
                    sessionManager,
                    getConfig().clientIdentifier(),
                    shardId,
                    clock.millis(),
                    getConfig().maxBatchSize(),
                    metrics.recordWrite());
        }
    }

    class ReadBatchFactory extends BatchFactory {
        public ReadBatchFactory(
                @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
                @NonNull ClientConfig config,
                @NonNull Clock clock,
                @NonNull BatchMetrics metrics) {
            super(stubByShardId, config, clock, metrics);
        }

        @Override
        public @NonNull Batch apply(@NonNull Long shardId) {
            return new ReadBatch(stubByShardId, shardId, clock.millis(), metrics.recordRead());
        }
    }
}
