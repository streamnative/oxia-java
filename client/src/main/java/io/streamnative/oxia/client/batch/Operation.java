package io.streamnative.oxia.client.batch;

import static io.streamnative.oxia.client.ProtoUtil.setOptionalExpectedVersion;
import static io.streamnative.oxia.client.batch.Operation.ReadOperation;
import static io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import static io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;

import io.streamnative.oxia.proto.DeleteRangeRequest;
import io.streamnative.oxia.proto.DeleteRequest;
import io.streamnative.oxia.proto.GetRequest;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.PutRequest;
import lombok.NonNull;

sealed interface Operation permits ReadOperation, WriteOperation {

    sealed interface ReadOperation extends Operation permits GetOperation, ListOperation {
        record GetOperation(@NonNull String key) implements ReadOperation {
            GetRequest toProto() {
                return GetRequest.newBuilder().setKey(key).build();
            }
        }

        record ListOperation(@NonNull String minKeyInclusive, @NonNull String maxKeyInclusive)
                implements ReadOperation {
            ListRequest toProto() {
                return ListRequest.newBuilder()
                        .setStartInclusive(minKeyInclusive)
                        .setEndExclusive(maxKeyInclusive)
                        .build();
            }
        }
    }

    sealed interface WriteOperation extends Operation
            permits PutOperation, DeleteOperation, DeleteRangeOperation {
        record PutOperation(@NonNull String key, byte @NonNull [] payload, long expectedVersion)
                implements WriteOperation {
            PutRequest toProto() {
                var builder = PutRequest.newBuilder().setKey(key);
                setOptionalExpectedVersion(expectedVersion, builder::setExpectedVersion);
                return builder.build();
            }
        }

        record DeleteOperation(@NonNull String key, long expectedVersion) implements WriteOperation {
            DeleteRequest toProto() {
                var builder = DeleteRequest.newBuilder().setKey(key);
                setOptionalExpectedVersion(expectedVersion, builder::setExpectedVersion);
                return builder.build();
            }
        }

        record DeleteRangeOperation(@NonNull String minKeyInclusive, @NonNull String maxKeyInclusive)
                implements WriteOperation {
            DeleteRangeRequest toProto() {
                return DeleteRangeRequest.newBuilder()
                        .setStartInclusive(minKeyInclusive)
                        .setEndExclusive(maxKeyInclusive)
                        .build();
            }
        }
    }
}
