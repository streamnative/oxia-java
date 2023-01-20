package io.streamnative.oxia.client.batch;

import static io.streamnative.oxia.client.ProtoUtil.setOptionalExpectedVersion;
import io.streamnative.oxia.proto.DeleteRangeRequest;
import io.streamnative.oxia.proto.DeleteRequest;
import io.streamnative.oxia.proto.GetRequest;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.PutRequest;
import lombok.NonNull;

sealed interface Operation permits Operation.ReadOperation, Operation.WriteOperation {

    sealed interface ReadOperation extends Operation
            permits Operation.ReadOperation.GetOperation, Operation.ReadOperation.ListOperation {
        record GetOperation(@NonNull String key) implements ReadOperation {
            GetRequest toProto() {
                return GetRequest.newBuilder().setKey(key).build();
            }
        }

        record ListOperation(@NonNull String minKeyInclusive, @NonNull String maxKeyInclusive)
                implements ReadOperation {
            ListRequest toProto() {
                return ListRequest.newBuilder().setStartInclusive(minKeyInclusive).setEndExclusive(maxKeyInclusive)
                        .build();
            }
        }
    }

    sealed interface WriteOperation extends Operation
            permits Operation.WriteOperation.PutOperation, Operation.WriteOperation.DeleteOperation,
            Operation.WriteOperation.DeleteRangeOperation {
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
                return DeleteRangeRequest.newBuilder().setStartInclusive(minKeyInclusive).setEndExclusive(maxKeyInclusive)
                        .build();
            }
        }
    }
}

