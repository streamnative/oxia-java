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

import static io.streamnative.oxia.client.ProtoUtil.VersionIdNotExists;
import static io.streamnative.oxia.client.ProtoUtil.setOptionalExpectedVersionId;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
                long expectedVersionId)
                implements WriteOperation<PutResult> {
            PutRequest toProto() {
                var builder = PutRequest.newBuilder().setKey(key).setPayload(ByteString.copyFrom(payload));
                setOptionalExpectedVersionId(expectedVersionId, builder::setExpectedVersion);
                return builder.build();
            }

            void complete(@NonNull PutResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION -> fail(new UnexpectedVersionIdException(expectedVersionId));
                    case OK -> callback.complete(PutResult.fromProto(response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            public PutOperation(
                    @NonNull CompletableFuture<PutResult> callback,
                    @NonNull String key,
                    byte @NonNull [] payload) {
                this(callback, key, payload, VersionIdNotExists);
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
                return expectedVersionId == that.expectedVersionId
                        && key.equals(that.key)
                        && Arrays.equals(payload, that.payload);
            }

            @Override
            public int hashCode() {
                int result = Objects.hash(key, expectedVersionId);
                result = 31 * result + Arrays.hashCode(payload);
                return result;
            }
        }

        record DeleteOperation(
                @NonNull CompletableFuture<Boolean> callback, @NonNull String key, long expectedVersionId)
                implements WriteOperation<Boolean> {
            DeleteRequest toProto() {
                var builder = DeleteRequest.newBuilder().setKey(key);
                setOptionalExpectedVersionId(expectedVersionId, builder::setExpectedVersion);
                return builder.build();
            }

            void complete(@NonNull DeleteResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION -> fail(new UnexpectedVersionIdException(expectedVersionId));
                    case KEY_NOT_FOUND -> callback.complete(false);
                    case OK -> callback.complete(true);
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            public DeleteOperation(@NonNull CompletableFuture<Boolean> callback, @NonNull String key) {
                this(callback, key, VersionIdNotExists);
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
