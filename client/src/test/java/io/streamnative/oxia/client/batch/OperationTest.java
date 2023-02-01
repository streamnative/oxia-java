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

import static io.streamnative.oxia.client.api.Version.KeyNotExists;
import static io.streamnative.oxia.proto.Status.KEY_NOT_FOUND;
import static io.streamnative.oxia.proto.Status.OK;
import static io.streamnative.oxia.proto.Status.UNEXPECTED_VERSION_ID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.UnexpectedVersionIdException;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.proto.DeleteRangeResponse;
import io.streamnative.oxia.proto.DeleteResponse;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.ListResponse;
import io.streamnative.oxia.proto.PutResponse;
import io.streamnative.oxia.proto.Version;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OperationTest {

    @Nested
    @DisplayName("Tests of get operation")
    class GetOperationTests {

        CompletableFuture<GetResult> callback = new CompletableFuture<>();
        GetOperation op = new GetOperation(callback, "key");

        @Test
        void toProto() {
            var request = op.toProto();
            assertThat(request.getKey()).isEqualTo(op.key());
        }

        @Test
        void completeKeyNotFound() {
            var response = GetResponse.newBuilder().setStatus(KEY_NOT_FOUND).build();
            op.complete(response);
            assertThat(callback).isCompletedWithValueMatching(Objects::isNull);
        }

        @Test
        void completeOk() {
            var payload = "hello".getBytes(UTF_8);
            var response =
                    GetResponse.newBuilder()
                            .setStatus(OK)
                            .setValue(ByteString.copyFrom(payload))
                            .setVersion(
                                    Version.newBuilder()
                                            .setVersionId(1L)
                                            .setCreatedTimestamp(2L)
                                            .setModifiedTimestamp(3L)
                                            .build())
                            .build();
            op.complete(response);
            assertThat(callback)
                    .isCompletedWithValueMatching(
                            r ->
                                    r.equals(
                                            new GetResult(
                                                    payload, new io.streamnative.oxia.client.api.Version(1L, 2L, 3L))));
        }

        @Test
        void completeOther() {
            var response = GetResponse.newBuilder().setStatusValue(-1).build();
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(IllegalStateException.class)
                                        .hasMessage("GRPC.Status: UNRECOGNIZED");
                            });
        }
    }

    @Nested
    @DisplayName("Tests of list operation")
    class ListOperationTests {
        CompletableFuture<List<String>> callback = new CompletableFuture<>();
        ListOperation op = new ListOperation(callback, "a", "b");

        @Test
        void toProto() {
            var request = op.toProto();
            assertThat(request.getStartInclusive()).isEqualTo(op.minKeyInclusive());
            assertThat(request.getEndExclusive()).isEqualTo(op.maxKeyInclusive());
        }

        @Test
        void completeOk() {
            var response = ListResponse.newBuilder().addAllKeys(List.of("a", "b", "c")).build();
            op.complete(response);
            assertThat(callback).isCompletedWithValueMatching(r -> r.equals(List.of("a", "b", "c")));
        }
    }

    @Nested
    @DisplayName("Tests of put operation")
    class PutOperationTests {
        CompletableFuture<PutResult> callback = new CompletableFuture<>();
        byte[] payload = "hello".getBytes(UTF_8);
        PutOperation op = new PutOperation(callback, "key", payload, 10L);

        @Test
        void constructInvalidExpectedVersionId() {
            assertThatNoException()
                    .isThrownBy(() -> new PutOperation(callback, "key", payload, KeyNotExists));
            assertThatNoException().isThrownBy(() -> new PutOperation(callback, "key", payload, 0L));
            assertThatThrownBy(() -> new PutOperation(callback, "key", payload, -2L))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void toProtoNoExpectedVersion() {
            var op = new PutOperation(callback, "key", payload, null);
            var request = op.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getValue().toByteArray()).isEqualTo(op.value());
                                assertThat(r.hasExpectedVersionId()).isFalse();
                            });
        }

        @Test
        void toProtoExpectedVersion() {
            var op = new PutOperation(callback, "key", payload, 1L);
            var request = op.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getValue().toByteArray()).isEqualTo(op.value());
                                assertThat(r.getExpectedVersionId()).isEqualTo(1L);
                            });
        }

        @Test
        void toProtoNoExistingVersion() {
            var op = new PutOperation(callback, "key", payload, KeyNotExists);
            var request = op.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getValue().toByteArray()).isEqualTo(op.value());
                                assertThat(r.getExpectedVersionId()).isEqualTo(KeyNotExists);
                            });
        }

        @Test
        void completeUnexpectedVersion() {
            var response = PutResponse.newBuilder().setStatus(UNEXPECTED_VERSION_ID).build();
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(UnexpectedVersionIdException.class)
                                        .hasMessage("key 'key' has unexpected versionId: 10");
                            });
        }

        @Test
        void completeKeyAlreadyExists() {
            var op = new PutOperation(callback, "key", payload, KeyNotExists);
            var response = PutResponse.newBuilder().setStatus(UNEXPECTED_VERSION_ID).build();
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(KeyAlreadyExistsException.class)
                                        .hasMessage("key already exists: key");
                            });
        }

        @Test
        void completeOk() {
            var response =
                    PutResponse.newBuilder()
                            .setStatus(OK)
                            .setVersion(
                                    Version.newBuilder()
                                            .setVersionId(1L)
                                            .setCreatedTimestamp(2L)
                                            .setModifiedTimestamp(3L)
                                            .build())
                            .build();
            op.complete(response);
            assertThat(callback)
                    .isCompletedWithValueMatching(
                            r ->
                                    r.equals(new PutResult(new io.streamnative.oxia.client.api.Version(1L, 2L, 3L))));
        }

        @Test
        void completeOther() {
            var response = PutResponse.newBuilder().setStatusValue(-1).build();
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(IllegalStateException.class)
                                        .hasMessage("GRPC.Status: UNRECOGNIZED");
                            });
        }
    }

    @Nested
    @DisplayName("Tests of delete operation")
    class DeleteOperationTests {
        CompletableFuture<Boolean> callback = new CompletableFuture<>();
        DeleteOperation op = new DeleteOperation(callback, "key", 10L);

        @Test
        void constructInvalidExpectedVersionId() {
            assertThatNoException().isThrownBy(() -> new DeleteOperation(callback, "key", 0L));
            assertThatThrownBy(() -> new DeleteOperation(callback, "key", KeyNotExists))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new DeleteOperation(callback, "key", -2L))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void toProtoNoExpectedVersion() {
            var op = new DeleteOperation(callback, "key");
            var request = op.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.hasExpectedVersionId()).isFalse();
                            });
        }

        @Test
        void toProtoExpectedVersion() {
            var op = new DeleteOperation(callback, "key", 1L);
            var request = op.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getExpectedVersionId()).isEqualTo(1L);
                            });
        }

        @Test
        void completeUnexpectedVersion() {
            var response = DeleteResponse.newBuilder().setStatus(UNEXPECTED_VERSION_ID).build();
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(UnexpectedVersionIdException.class)
                                        .hasMessage("key 'key' has unexpected versionId: 10");
                            });
        }

        @Test
        void completeOk() {
            var response = DeleteResponse.newBuilder().setStatus(OK).build();
            op.complete(response);
            assertThat(callback).isCompletedWithValueMatching(r -> r);
        }

        @Test
        void completeKeyNotFound() {
            var response = DeleteResponse.newBuilder().setStatus(KEY_NOT_FOUND).build();
            op.complete(response);
            assertThat(callback).isCompletedWithValueMatching(r -> !r);
        }

        @Test
        void completeOther() {
            var response = DeleteResponse.newBuilder().setStatusValue(-1).build();
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(IllegalStateException.class)
                                        .hasMessage("GRPC.Status: UNRECOGNIZED");
                            });
        }
    }

    @Nested
    @DisplayName("Tests of delete range operation")
    class DeleteRangeOperationTests {
        CompletableFuture<Void> callback = new CompletableFuture<>();
        DeleteRangeOperation op = new DeleteRangeOperation(callback, "a", "b");

        @Test
        void toProto() {
            var request = op.toProto();
            assertThat(request.getStartInclusive()).isEqualTo(op.minKeyInclusive());
            assertThat(request.getEndExclusive()).isEqualTo(op.maxKeyInclusive());
        }

        @Test
        void completeOk() {
            var response = DeleteRangeResponse.newBuilder().setStatus(OK).build();
            op.complete(response);
            assertThat(callback).isCompleted();
        }

        @Test
        void completeOther() {
            var response = DeleteRangeResponse.newBuilder().setStatusValue(-1).build();
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(IllegalStateException.class)
                                        .hasMessage("GRPC.Status: UNRECOGNIZED");
                            });
        }
    }
}
