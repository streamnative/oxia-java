/*
 * Copyright © 2022-2024 StreamNative Inc.
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

import static io.streamnative.oxia.client.api.Version.KeyNotExists;
import static io.streamnative.oxia.client.batch.Operation.CloseOperation;
import static io.streamnative.oxia.client.batch.Operation.ReadOperation;
import static io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import static io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;

import com.google.protobuf.ByteString;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.SessionDoesNotExistException;
import io.streamnative.oxia.client.api.UnexpectedVersionIdException;
import io.streamnative.oxia.proto.DeleteRangeRequest;
import io.streamnative.oxia.proto.DeleteRangeResponse;
import io.streamnative.oxia.proto.DeleteRequest;
import io.streamnative.oxia.proto.DeleteResponse;
import io.streamnative.oxia.proto.GetRequest;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.PutRequest;
import io.streamnative.oxia.proto.PutResponse;
import io.streamnative.oxia.proto.Status;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public sealed interface Operation<R> permits CloseOperation, ReadOperation, WriteOperation {

    CompletableFuture<R> callback();

    long sequence();

    default void fail(Throwable t) {
        callback().completeExceptionally(t);
    }

    sealed interface ReadOperation<R> extends Operation<R> permits GetOperation {
        record GetOperation(
                long sequence, @NonNull CompletableFuture<GetResult> callback, @NonNull String key)
                implements ReadOperation<GetResult> {
            GetRequest toProto() {
                return GetRequest.newBuilder().setKey(key).setIncludeValue(true).build();
            }

            void complete(@NonNull GetResponse response) {
                switch (response.getStatus()) {
                    case KEY_NOT_FOUND -> callback.complete(null);
                    case OK -> callback.complete(GetResult.fromProto(response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }
        }
    }

    sealed interface WriteOperation<R> extends Operation<R>
            permits PutOperation, DeleteOperation, DeleteRangeOperation {
        record PutOperation(
                long sequence,
                @NonNull CompletableFuture<PutResult> callback,
                @NonNull String key,
                byte @NonNull [] value,
                @NonNull Optional<Long> expectedVersionId,
                boolean ephemeral)
                implements WriteOperation<PutResult> {

            public PutOperation {
                if (expectedVersionId.isPresent() && expectedVersionId.get() < KeyNotExists) {
                    throw new IllegalArgumentException(
                            "expectedVersionId must be >= -1 (KeyNotExists), was: " + expectedVersionId.get());
                }
            }

            PutRequest toProto(@NonNull Optional<SessionInfo> sessionInfo) {
                var builder = PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFrom(value));
                expectedVersionId.ifPresent(builder::setExpectedVersionId);
                if (ephemeral) {
                    if (sessionInfo.isPresent()) {
                        builder
                                .setSessionId(sessionInfo.get().sessionId())
                                .setClientIdentity(sessionInfo.get().clientIdentifier());
                    } else {
                        throw new IllegalStateException("session context required for ephemeral operation");
                    }
                }
                return builder.build();
            }

            void complete(@NonNull PutResponse response) {
                switch (response.getStatus()) {
                    case SESSION_DOES_NOT_EXIST -> fail(new SessionDoesNotExistException());
                    case UNEXPECTED_VERSION_ID -> {
                        if (expectedVersionId.get() == KeyNotExists) {
                            fail(new KeyAlreadyExistsException(key));
                        } else {
                            fail(new UnexpectedVersionIdException(key, expectedVersionId.get()));
                        }
                    }
                    case OK -> callback.complete(PutResult.fromProto(response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
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
                return key.equals(that.key)
                        && Arrays.equals(value, that.value)
                        && Objects.equals(expectedVersionId, that.expectedVersionId);
            }

            @Override
            public int hashCode() {
                int result = Objects.hash(key, expectedVersionId);
                result = 31 * result + Arrays.hashCode(value);
                return result;
            }

            record SessionInfo(long sessionId, @NonNull String clientIdentifier) {}
        }

        record DeleteOperation(
                long sequence,
                @NonNull CompletableFuture<Boolean> callback,
                @NonNull String key,
                @NonNull Optional<Long> expectedVersionId)
                implements WriteOperation<Boolean> {

            public DeleteOperation {
                if (expectedVersionId.isPresent() && expectedVersionId.get() < 0) {
                    throw new IllegalArgumentException(
                            "expectedVersionId must be >= 0, was: " + expectedVersionId.get());
                }
            }

            DeleteRequest toProto() {
                var builder = DeleteRequest.newBuilder().setKey(key);
                expectedVersionId.ifPresent(builder::setExpectedVersionId);
                return builder.build();
            }

            void complete(@NonNull DeleteResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION_ID -> fail(
                            new UnexpectedVersionIdException(key, expectedVersionId.get()));
                    case KEY_NOT_FOUND -> callback.complete(false);
                    case OK -> callback.complete(true);
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            public DeleteOperation(
                    long sequence, @NonNull CompletableFuture<Boolean> callback, @NonNull String key) {
                this(sequence, callback, key, Optional.empty());
            }
        }

        record DeleteRangeOperation(
                long sequence,
                @NonNull CompletableFuture<Void> callback,
                @NonNull String startKeyInclusive,
                @NonNull String endKeyExclusive)
                implements WriteOperation<Void> {
            DeleteRangeRequest toProto() {
                return DeleteRangeRequest.newBuilder()
                        .setStartInclusive(startKeyInclusive)
                        .setEndExclusive(endKeyExclusive)
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

    enum CloseOperation implements Operation<Void> {
        INSTANCE {
            @Override
            public long sequence() {
                return Long.MIN_VALUE;
            }
        };

        @Override
        public CompletableFuture<Void> callback() {
            return null;
        }
    }

    Comparator<Operation> PriorityComparator =
            (o1, o2) -> {
                if (o1 == CloseOperation.INSTANCE) {
                    return -1;
                } else if (o2 == CloseOperation.INSTANCE) {
                    return +1;
                } else {
                    return Long.compare(o1.sequence(), o2.sequence());
                }
            };
}
