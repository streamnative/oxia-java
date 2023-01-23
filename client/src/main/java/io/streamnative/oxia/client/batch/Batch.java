package io.streamnative.oxia.client.batch;

import static io.streamnative.oxia.client.ProtoUtil.longToUint32;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PUBLIC;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
import io.streamnative.oxia.proto.ReadRequest;
import io.streamnative.oxia.proto.WriteRequest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

sealed interface Batch permits Batch.WriteBatch, Batch.ReadBatch {

    long getStartTime();

    void add(@NonNull Operation<?> operation);

    int size();

    long getShardId();

    @NonNull
    Duration getRequestTimeout();

    void complete();

    final class WriteBatch extends BatchBase implements Batch {
        private final List<PutOperation> puts = new ArrayList<>();
        private final List<DeleteOperation> deletes = new ArrayList<>();
        private final List<DeleteRangeOperation> deleteRanges = new ArrayList<>();

        WriteBatch(
                @NonNull Supplier<OxiaClientBlockingStub> clientSupplier,
                long shardId,
                long createTime,
                @NonNull Duration requestTimeout) {
            super(clientSupplier, shardId, createTime, requestTimeout);
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
            var response = getClientSupplier().get().write(toProto());
            for (var i = 0; i < deletes.size(); i++) {
                deletes.get(i).complete(response.getDeletes(i));
            }
            for (var i = 0; i < deleteRanges.size(); i++) {
                deleteRanges.get(i).complete(response.getDeleteRanges(i));
            }
            for (var i = 0; i < puts.size(); i++) {
                puts.get(i).complete(response.getPuts(i));
            }
        }

        @NonNull
        WriteRequest toProto() {
            return WriteRequest.newBuilder()
                    .setShardId(longToUint32(getShardId()))
                    .addAllPuts(puts.stream().map(PutOperation::toProto).collect(toList()))
                    .addAllDeletes(deletes.stream().map(DeleteOperation::toProto).collect(toList()))
                    .addAllDeleteRanges(
                            deleteRanges.stream().map(DeleteRangeOperation::toProto).collect(toList()))
                    .build();
        }
    }

    final class ReadBatch extends BatchBase implements Batch {
        private final List<GetOperation> gets = new ArrayList<>();
        private final List<ListOperation> lists = new ArrayList<>();

        private Instant time;

        public void add(@NonNull Operation<?> operation) {
            if (operation instanceof GetOperation g) {
                gets.add(g);
            } else if (operation instanceof ListOperation l) {
                lists.add(l);
            }
        }

        ReadBatch(
                @NonNull Supplier<OxiaClientBlockingStub> clientSupplier,
                long shardId,
                long createTime,
                @NonNull Duration requestTimeout) {
            super(clientSupplier, shardId, createTime, requestTimeout);
        }

        @Override
        public int size() {
            return gets.size() + lists.size();
        }

        @Override
        public void complete() {
            var response = getClientSupplier().get().read(toProto());
            for (var i = 0; i < gets.size(); i++) {
                gets.get(i).complete(response.getGets(i));
            }
            for (var i = 0; i < lists.size(); i++) {
                lists.get(i).complete(response.getLists(i));
            }
        }

        @NonNull
        ReadRequest toProto() {
            return ReadRequest.newBuilder()
                    .setShardId(longToUint32(getShardId()))
                    .addAllGets(gets.stream().map(GetOperation::toProto).collect(toList()))
                    .addAllLists(lists.stream().map(ListOperation::toProto).collect(toList()))
                    .build();
        }
    }

    @RequiredArgsConstructor(access = PRIVATE)
    abstract class BatchBase {
        @Getter private final @NonNull Supplier<OxiaClientBlockingStub> clientSupplier;
        @Getter private final long shardId;
        @Getter private final long startTime;
        @Getter private final @NonNull Duration requestTimeout;

        @Getter
        @Setter(PUBLIC)
        private Exception failure;
    }

    @RequiredArgsConstructor(access = PACKAGE)
    abstract class BatchFactory implements Function<Long, Batch> {
        final @NonNull Supplier<OxiaClientBlockingStub> clientSupplier;

        @Getter(PACKAGE)
        private final @NonNull ClientConfig config;

        final @NonNull Clock clock;

        public abstract @NonNull Batch apply(@NonNull Long shardId);
    }

    class WriteBatchFactory extends BatchFactory {
        WriteBatchFactory(
                @NonNull Supplier<OxiaClientBlockingStub> clientSupplier,
                @NonNull ClientConfig config,
                @NonNull Clock clock) {
            super(clientSupplier, config, clock);
        }

        @Override
        public @NonNull Batch apply(@NonNull Long shardId) {
            return new WriteBatch(clientSupplier, shardId, clock.millis(), getConfig().requestTimeout());
        }
    }

    class ReadBatchFactory extends BatchFactory {
        ReadBatchFactory(
                @NonNull Supplier<OxiaClientBlockingStub> clientSupplier,
                @NonNull ClientConfig ctx,
                @NonNull Clock clock) {
            super(clientSupplier, ctx, clock);
        }

        @Override
        public @NonNull Batch apply(@NonNull Long shardId) {
            return new ReadBatch(clientSupplier, shardId, clock.millis(), getConfig().requestTimeout());
        }
    }
}
