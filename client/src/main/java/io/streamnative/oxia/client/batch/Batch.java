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

import static io.streamnative.oxia.client.ProtoUtil.longToUint32;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
import io.streamnative.oxia.proto.ReadRequest;
import io.streamnative.oxia.proto.WriteRequest;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public interface Batch {

    long getStartTime();

    void add(@NonNull Operation<?> operation);

    int size();

    long getShardId();

    void complete();

    final class WriteBatch extends BatchBase implements Batch {
        @VisibleForTesting final List<PutOperation> puts = new ArrayList<>();
        @VisibleForTesting final List<DeleteOperation> deletes = new ArrayList<>();
        @VisibleForTesting final List<DeleteRangeOperation> deleteRanges = new ArrayList<>();
        private final SessionManager sessionManager;

        WriteBatch(
                @NonNull Function<Long, OxiaClientBlockingStub> stubByShardId,
                @NonNull SessionManager sessionManager,
                long shardId,
                long createTime) {
            super(stubByShardId, shardId, createTime);
            this.sessionManager = sessionManager;
        }

        public void add(@NonNull Operation<?> operation) {
            if (operation instanceof PutOperation p) {
                puts.add(p);
            } else if (operation instanceof DeleteOperation d) {
                deletes.add(d);
            } else if (operation instanceof DeleteRangeOperation r) {
                deleteRanges.add(r);
            }
        }

        @Override
        public int size() {
            return puts.size() + deletes.size() + deleteRanges.size();
        }

        @Override
        public void complete() {
            try {
                var response = getStubByShardId().apply(getShardId()).write(toProto());
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
                deletes.forEach(d -> d.fail(batchError));
                deleteRanges.forEach(f -> f.fail(batchError));
                puts.forEach(p -> p.fail(batchError));
            }
        }

        @NonNull
        WriteRequest toProto() {
            var sessionId = sessionManager.getSessionId(getShardId());
            return WriteRequest.newBuilder()
                    .setShardId(longToUint32(getShardId()))
                    .addAllPuts(puts.stream().map(p -> p.toProto(sessionId)).collect(toList()))
                    .addAllDeletes(deletes.stream().map(DeleteOperation::toProto).collect(toList()))
                    .addAllDeleteRanges(
                            deleteRanges.stream().map(DeleteRangeOperation::toProto).collect(toList()))
                    .build();
        }
    }

    final class ReadBatch extends BatchBase implements Batch {
        @VisibleForTesting final List<GetOperation> gets = new ArrayList<>();

        public void add(@NonNull Operation<?> operation) {
            if (operation instanceof GetOperation g) {
                gets.add(g);
            }
        }

        ReadBatch(
                @NonNull Function<Long, OxiaClientBlockingStub> stubByShardId,
                long shardId,
                long createTime) {
            super(stubByShardId, shardId, createTime);
        }

        @Override
        public int size() {
            return gets.size();
        }

        @Override
        public void complete() {
            try {
                var response = getStubByShardId().apply(getShardId()).read(toProto());
                for (var i = 0; i < gets.size(); i++) {
                    gets.get(i).complete(response.getGets(i));
                }
            } catch (Throwable batchError) {
                gets.forEach(g -> g.fail(batchError));
            }
        }

        @NonNull
        ReadRequest toProto() {
            return ReadRequest.newBuilder()
                    .setShardId(longToUint32(getShardId()))
                    .addAllGets(gets.stream().map(GetOperation::toProto).collect(toList()))
                    .build();
        }
    }

    @RequiredArgsConstructor(access = PRIVATE)
    abstract class BatchBase {
        @Getter private final @NonNull Function<Long, OxiaClientBlockingStub> stubByShardId;
        @Getter private final long shardId;
        @Getter private final long startTime;
    }

    @RequiredArgsConstructor(access = PACKAGE)
    abstract class BatchFactory implements Function<Long, Batch> {
        final @NonNull Function<Long, OxiaClientBlockingStub> stubByShardId;

        @Getter(PACKAGE)
        private final @NonNull ClientConfig config;

        final @NonNull Clock clock;

        public abstract @NonNull Batch apply(@NonNull Long shardId);
    }

    class WriteBatchFactory extends BatchFactory {
        final @NonNull SessionManager sessionManager;

        public WriteBatchFactory(
                @NonNull Function<Long, OxiaClientBlockingStub> stubByShardId,
                @NonNull SessionManager sessionManager,
                @NonNull ClientConfig config,
                @NonNull Clock clock) {
            super(stubByShardId, config, clock);
            this.sessionManager = sessionManager;
        }

        @Override
        public @NonNull Batch apply(@NonNull Long shardId) {
            return new WriteBatch(stubByShardId, sessionManager, shardId, clock.millis());
        }
    }

    class ReadBatchFactory extends BatchFactory {
        public ReadBatchFactory(
                @NonNull Function<Long, OxiaClientBlockingStub> stubByShardId,
                @NonNull ClientConfig config,
                @NonNull Clock clock) {
            super(stubByShardId, config, clock);
        }

        @Override
        public @NonNull Batch apply(@NonNull Long shardId) {
            return new ReadBatch(stubByShardId, shardId, clock.millis());
        }
    }
}
