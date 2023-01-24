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
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

import com.google.protobuf.ByteString;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.KeyNotFoundException;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.UnexpectedVersionException;
import io.streamnative.oxia.proto.DeleteRangeRequest;
import io.streamnative.oxia.proto.DeleteRangeResponse;
import io.streamnative.oxia.proto.DeleteRequest;
import io.streamnative.oxia.proto.DeleteResponse;
import io.streamnative.oxia.proto.GetRequest;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.ListResponse;
import io.streamnative.oxia.proto.PutRequest;
import io.streamnative.oxia.proto.PutResponse;
import io.streamnative.oxia.proto.Status;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public sealed interface Operation<R> permits ReadOperation, WriteOperation {

    CompletableFuture<R> callback();

    default void fail(Throwable t) {
        callback().completeExceptionally(t);
    }

    sealed interface ReadOperation<R> extends Operation<R> permits GetOperation, ListOperation {
        record GetOperation(@NonNull CompletableFuture<GetResult> callback, @NonNull String key)
                implements ReadOperation<GetResult> {
            GetRequest toProto() {
                return GetRequest.newBuilder().setKey(key).build();
            }

            void complete(@NonNull GetResponse response) {
                switch (response.getStatus()) {
                    case KEY_NOT_FOUND -> fail(new KeyNotFoundException(key));
                    case OK -> callback.complete(GetResult.fromProto(response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }
        }

        record ListOperation(
                @NonNull CompletableFuture<List<String>> callback,
                @NonNull String minKeyInclusive,
                @NonNull String maxKeyInclusive)
                implements ReadOperation<List<String>> {
            ListRequest toProto() {
                return ListRequest.newBuilder()
                        .setStartInclusive(minKeyInclusive)
                        .setEndExclusive(maxKeyInclusive)
                        .build();
            }

            void complete(@NonNull ListResponse response) {
                callback.complete(
                        unmodifiableList(
                                response.getKeysList().asByteStringList().stream()
                                        .map(ByteString::toStringUtf8)
                                        .collect(toList())));
            }
        }
    }

    sealed interface WriteOperation<R> extends Operation<R>
            permits PutOperation, DeleteOperation, DeleteRangeOperation {
        record PutOperation(
                @NonNull CompletableFuture<PutResult> callback,
                @NonNull String key,
                byte @NonNull [] payload,
                long expectedVersion)
                implements WriteOperation<PutResult> {
            PutRequest toProto() {
                var builder = PutRequest.newBuilder().setKey(key).setPayload(ByteString.copyFrom(payload));
                setOptionalExpectedVersion(expectedVersion, builder::setExpectedVersion);
                return builder.build();
            }

            void complete(@NonNull PutResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION -> fail(new UnexpectedVersionException(expectedVersion));
                    case OK -> callback.complete(PutResult.fromProto(response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            public PutOperation(
                    @NonNull CompletableFuture<PutResult> callback,
                    @NonNull String key,
                    byte @NonNull [] payload) {
                this(callback, key, payload, versionNotExists);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                PutOperation that = (PutOperation) o;
                return expectedVersion == that.expectedVersion
                        && key.equals(that.key)
                        && Arrays.equals(payload, that.payload);
            }
        }

        record DeleteOperation(
                @NonNull CompletableFuture<Boolean> callback, @NonNull String key, long expectedVersion)
                implements WriteOperation<Boolean> {
            DeleteRequest toProto() {
                var builder = DeleteRequest.newBuilder().setKey(key);
                setOptionalExpectedVersion(expectedVersion, builder::setExpectedVersion);
                return builder.build();
            }

            void complete(@NonNull DeleteResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION -> fail(new UnexpectedVersionException(expectedVersion));
                    case KEY_NOT_FOUND -> callback.complete(false);
                    case OK -> callback.complete(true);
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            public DeleteOperation(@NonNull CompletableFuture<Boolean> callback, @NonNull String key) {
                this(callback, key, versionNotExists);
            }
        }

        record DeleteRangeOperation(
                @NonNull CompletableFuture<Void> callback,
                @NonNull String minKeyInclusive,
                @NonNull String maxKeyInclusive)
                implements WriteOperation<Void> {
            DeleteRangeRequest toProto() {
                return DeleteRangeRequest.newBuilder()
                        .setStartInclusive(minKeyInclusive)
                        .setEndExclusive(maxKeyInclusive)
                        .build();
            }

            void complete(@NonNull DeleteRangeResponse response) {
                if (response.getStatus() == Status.OK) {
                    callback.complete(null);
                } else {
                    fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }
        }
    }
}
