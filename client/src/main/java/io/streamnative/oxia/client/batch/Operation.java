package io.streamnative.oxia.client.batch;

import static io.streamnative.oxia.client.ProtoUtil.setOptionalExpectedVersion;
import static io.streamnative.oxia.client.ProtoUtil.versionNotExists;
import static io.streamnative.oxia.client.batch.Operation.ReadOperation;
import static io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import static io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;

import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.proto.DeleteRangeRequest;
import io.streamnative.oxia.proto.DeleteRequest;
import io.streamnative.oxia.proto.GetRequest;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.PutRequest;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public sealed interface Operation<R> permits ReadOperation, WriteOperation {

    default CompletableFuture<R> complete(Batcher batcher) {
        return batcher.add(this);
    }

    sealed interface ReadOperation<R> extends Operation<R> permits GetOperation, ListOperation {
        record GetOperation(@NonNull String key) implements ReadOperation<GetResult> {
            GetRequest toProto() {
                return GetRequest.newBuilder().setKey(key).build();
            }
        }

        record ListOperation(@NonNull String minKeyInclusive, @NonNull String maxKeyInclusive)
                implements ReadOperation<List<String>> {
            ListRequest toProto() {
                return ListRequest.newBuilder()
                        .setStartInclusive(minKeyInclusive)
                        .setEndExclusive(maxKeyInclusive)
                        .build();
            }
        }
    }

    sealed interface WriteOperation<R> extends Operation<R>
            permits PutOperation, DeleteOperation, DeleteRangeOperation {
        record PutOperation(@NonNull String key, byte @NonNull [] payload, long expectedVersion)
                implements WriteOperation<PutResult> {
            PutRequest toProto() {
                var builder = PutRequest.newBuilder().setKey(key);
                setOptionalExpectedVersion(expectedVersion, builder::setExpectedVersion);
                return builder.build();
            }

            public PutOperation(@NonNull String key, byte @NonNull [] payload) {
                this(key, payload, versionNotExists);
            }
        }

        record DeleteOperation(@NonNull String key, long expectedVersion)
                implements WriteOperation<Boolean> {
            DeleteRequest toProto() {
                var builder = DeleteRequest.newBuilder().setKey(key);
                setOptionalExpectedVersion(expectedVersion, builder::setExpectedVersion);
                return builder.build();
            }

            public DeleteOperation(@NonNull String key) {
                this(key, versionNotExists);
            }
        }

        record DeleteRangeOperation(@NonNull String minKeyInclusive, @NonNull String maxKeyInclusive)
                implements WriteOperation<Void> {
            DeleteRangeRequest toProto() {
                return DeleteRangeRequest.newBuilder()
                        .setStartInclusive(minKeyInclusive)
                        .setEndExclusive(maxKeyInclusive)
                        .build();
            }
        }
    }
}
