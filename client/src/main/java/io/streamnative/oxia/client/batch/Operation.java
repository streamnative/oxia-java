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

import static io.streamnative.oxia.client.ProtoUtil.setOptionalExpectedVersionId;
import static io.streamnative.oxia.client.api.Version.KeyNotExists;
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
import io.streamnative.oxia.client.api.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.UnexpectedVersionIdException;
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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;

public sealed interface Operation<R> permits ReadOperation, WriteOperation {

    CompletableFuture<R> callback();

    int protoSize();

    default void fail(Throwable t) {
        callback().completeExceptionally(t);
    }

    sealed interface ReadOperation<R> extends Operation<R> permits GetOperation, ListOperation {
        final class GetOperation implements ReadOperation<GetResult> {
            private final @NonNull CompletableFuture<GetResult> callback;
            private final @NonNull String key;

            @Getter(lazy = true)
            private final GetRequest proto = toProto();

            public GetOperation(@NonNull CompletableFuture<GetResult> callback, @NonNull String key) {
                this.callback = callback;
                this.key = key;
            }

            private GetRequest toProto() {
                return GetRequest.newBuilder().setKey(key).setIncludeValue(true).build();
            }

            void complete(@NonNull GetResponse response) {
                switch (response.getStatus()) {
                    case KEY_NOT_FOUND -> callback.complete(null);
                    case OK -> callback.complete(GetResult.fromProto(response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            @Override
            public @NonNull CompletableFuture<GetResult> callback() {
                return callback;
            }

            @Override
            public int protoSize() {
                return getProto().getSerializedSize();
            }

            public @NonNull String key() {
                return key;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                GetOperation that = (GetOperation) o;
                return key.equals(that.key);
            }

            @Override
            public int hashCode() {
                return Objects.hash(key);
            }

            @Override
            public String toString() {
                return "GetOperation{" + "key='" + key + '\'' + '}';
            }
        }

        final class ListOperation implements ReadOperation<List<String>> {
            private final @NonNull CompletableFuture<List<String>> callback;
            private final @NonNull String minKeyInclusive;
            private final @NonNull String maxKeyInclusive;

            @Getter(lazy = true)
            private final ListRequest proto = toProto();

            public ListOperation(
                    @NonNull CompletableFuture<List<String>> callback,
                    @NonNull String minKeyInclusive,
                    @NonNull String maxKeyInclusive) {
                this.callback = callback;
                this.minKeyInclusive = minKeyInclusive;
                this.maxKeyInclusive = maxKeyInclusive;
            }

            private ListRequest toProto() {
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

            @Override
            public @NonNull CompletableFuture<List<String>> callback() {
                return callback;
            }

            @Override
            public int protoSize() {
                return getProto().getSerializedSize();
            }

            public @NonNull String minKeyInclusive() {
                return minKeyInclusive;
            }

            public @NonNull String maxKeyInclusive() {
                return maxKeyInclusive;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                ListOperation that = (ListOperation) o;
                return minKeyInclusive.equals(that.minKeyInclusive)
                        && maxKeyInclusive.equals(that.maxKeyInclusive);
            }

            @Override
            public int hashCode() {
                return Objects.hash(minKeyInclusive, maxKeyInclusive);
            }

            @Override
            public String toString() {
                return "ListOperation{"
                        + "minKeyInclusive='"
                        + minKeyInclusive
                        + '\''
                        + ", maxKeyInclusive='"
                        + maxKeyInclusive
                        + '\''
                        + '}';
            }
        }
    }

    sealed interface WriteOperation<R> extends Operation<R>
            permits PutOperation, DeleteOperation, DeleteRangeOperation {

        final class PutOperation implements WriteOperation<PutResult> {
            private final @NonNull CompletableFuture<PutResult> callback;
            private final @NonNull String key;
            private final byte @NonNull [] value;
            private final Long expectedVersionId;

            @Getter(lazy = true)
            private final PutRequest proto = toProto();

            public PutOperation(
                    @NonNull CompletableFuture<PutResult> callback,
                    @NonNull String key,
                    byte @NonNull [] value,
                    Long expectedVersionId) {
                if (expectedVersionId != null && expectedVersionId < KeyNotExists) {
                    throw new IllegalArgumentException(
                            "expectedVersionId must be >= -1 (KeyNotExists), was: " + expectedVersionId);
                }
                this.callback = callback;
                this.key = key;
                this.value = value;
                this.expectedVersionId = expectedVersionId;
            }

            public PutOperation(
                    @NonNull CompletableFuture<PutResult> callback,
                    @NonNull String key,
                    byte @NonNull [] payload) {
                this(callback, key, payload, null);
            }

            void complete(@NonNull PutResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION_ID -> {
                        if (expectedVersionId == KeyNotExists) {
                            fail(new KeyAlreadyExistsException(key));
                        } else {
                            fail(new UnexpectedVersionIdException(key, expectedVersionId));
                        }
                    }
                    case OK -> callback.complete(PutResult.fromProto(response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            private PutRequest toProto() {
                var builder = PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFrom(value));
                setOptionalExpectedVersionId(expectedVersionId, builder::setExpectedVersionId);
                return builder.build();
            }

            @Override
            public @NonNull CompletableFuture<PutResult> callback() {
                return callback;
            }

            @Override
            public int protoSize() {
                return getProto().getSerializedSize();
            }

            public @NonNull String key() {
                return key;
            }

            public byte @NonNull [] value() {
                return value;
            }

            public Long expectedVersionId() {
                return expectedVersionId;
            }
        }

        final class DeleteOperation implements WriteOperation<Boolean> {
            private final @NonNull CompletableFuture<Boolean> callback;
            private final @NonNull String key;
            private final Long expectedVersionId;

            @Getter(lazy = true)
            private final DeleteRequest proto = toProto();

            public DeleteOperation(
                    @NonNull CompletableFuture<Boolean> callback,
                    @NonNull String key,
                    Long expectedVersionId) {
                if (expectedVersionId != null && expectedVersionId < 0) {
                    throw new IllegalArgumentException(
                            "expectedVersionId must be >= 0, was: " + expectedVersionId);
                }
                this.callback = callback;
                this.key = key;
                this.expectedVersionId = expectedVersionId;
            }

            public DeleteOperation(@NonNull CompletableFuture<Boolean> callback, @NonNull String key) {
                this(callback, key, null);
            }

            void complete(@NonNull DeleteResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION_ID -> fail(
                            new UnexpectedVersionIdException(key(), expectedVersionId()));
                    case KEY_NOT_FOUND -> callback.complete(false);
                    case OK -> callback.complete(true);
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            private DeleteRequest toProto() {
                var builder = DeleteRequest.newBuilder().setKey(key);
                setOptionalExpectedVersionId(expectedVersionId, builder::setExpectedVersionId);
                return builder.build();
            }

            @Override
            public @NonNull CompletableFuture<Boolean> callback() {
                return callback;
            }

            @Override
            public int protoSize() {
                return getProto().getSerializedSize();
            }

            public @NonNull String key() {
                return key;
            }

            public Long expectedVersionId() {
                return expectedVersionId;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                DeleteOperation that = (DeleteOperation) o;
                return key.equals(that.key) && Objects.equals(expectedVersionId, that.expectedVersionId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(key, expectedVersionId);
            }

            @Override
            public String toString() {
                return "DeleteOperation{"
                        + "key='"
                        + key
                        + '\''
                        + ", expectedVersionId="
                        + expectedVersionId
                        + '}';
            }
        }

        final class DeleteRangeOperation implements WriteOperation<Void> {
            private final @NonNull CompletableFuture<Void> callback;
            private final @NonNull String minKeyInclusive;
            private final @NonNull String maxKeyInclusive;

            @Getter(lazy = true)
            private final DeleteRangeRequest proto = toProto();

            public DeleteRangeOperation(
                    @NonNull CompletableFuture<Void> callback,
                    @NonNull String minKeyInclusive,
                    @NonNull String maxKeyInclusive) {
                this.callback = callback;
                this.minKeyInclusive = minKeyInclusive;
                this.maxKeyInclusive = maxKeyInclusive;
            }

            private DeleteRangeRequest toProto() {
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

            @Override
            public @NonNull CompletableFuture<Void> callback() {
                return callback;
            }

            @Override
            public int protoSize() {
                return getProto().getSerializedSize();
            }

            public @NonNull String minKeyInclusive() {
                return minKeyInclusive;
            }

            public @NonNull String maxKeyInclusive() {
                return maxKeyInclusive;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                DeleteRangeOperation that = (DeleteRangeOperation) o;
                return minKeyInclusive.equals(that.minKeyInclusive)
                        && maxKeyInclusive.equals(that.maxKeyInclusive);
            }

            @Override
            public int hashCode() {
                return Objects.hash(minKeyInclusive, maxKeyInclusive);
            }

            @Override
            public String toString() {
                return "DeleteRangeOperation{"
                        + "minKeyInclusive='"
                        + minKeyInclusive
                        + '\''
                        + ", maxKeyInclusive='"
                        + maxKeyInclusive
                        + '\''
                        + '}';
            }
        }
    }
}
