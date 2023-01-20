package io.streamnative.oxia.client.batch;

import static io.streamnative.oxia.client.ProtoUtil.longToUint32;
import static java.util.stream.Collectors.toList;
import io.streamnative.oxia.client.ProtoUtil;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.proto.PutRequest;
import io.streamnative.oxia.proto.ReadRequest;
import io.streamnative.oxia.proto.WriteRequest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;

sealed interface Batch permits Batch.WriteBatch, Batch.ReadBatch {

    boolean isComplete();

    int size();

    int getShardId();

    Duration getRequestTimeout();

    final class WriteBatch extends BatchBase implements Batch {
        private final List<PutOperation> puts = new ArrayList<>();
        private final List<DeleteOperation> deletes = new ArrayList<>();
        private final List<DeleteRangeOperation> deleteRanges = new ArrayList<>();

        WriteBatch(long shardId, Duration requestTimeout) {
            super(shardId, requestTimeout);
        }

        void add(PutOperation operation) {
            puts.add(operation);
        }

        void add(DeleteOperation operation) {
            deletes.add(operation);
        }

        void add(DeleteRangeOperation operation) {
            deleteRanges.add(operation);
        }

        @Override
        public boolean isComplete() {
            return false;
        }

        @Override
        public int size() {
            return puts.size() + deletes.size() + deleteRanges.size();
        }

        WriteRequest toProto() {
            return WriteRequest.newBuilder()
                    .setShardId(longToUint32(getShardId())
                    .addAllPuts(puts.stream().map(PutOperation::toProto).collect(toList()))
                    .addAllDeletes(deletes.stream().map(DeleteOperation::toProto).collect(toList()))
                    .addAllDeleteRanges(deleteRanges.stream().map(DeleteRangeOperation::toProto).collect(toList()))
                    .build();
        }
    }

    final class ReadBatch extends BatchBase implements Batch {
        private final List<GetOperation> gets = new ArrayList<>();
        private final List<ListOperation> lists = new ArrayList<>();

        private Instant time;

        void add(GetOperation operation) {
            gets.add(operation);
        }

        void add(ListOperation operation) {
            lists.add(operation);
        }

        ReadBatch(long shardId, Duration requestTimeout) {
            super(shardId, requestTimeout);
        }

        @Override
        public boolean isComplete() {
            return false;
        }

        @Override
        public int size() {
            return gets.size() + lists.size();
        }

        ReadRequest toProto() {
            return ReadRequest.newBuilder()
                    .setShardId(longToUint32(getShardId())
                            .addAllGets(gets.stream().map(GetOperation::toProto).collect(toList()))
                            .addAllLists(lists.stream().map(ListOperation::toProto).collect(toList()))
                            .build();
        }
    }

    @Value
    abstract class BatchBase {
        long shardId;
        Duration requestTimeout;
    }
}