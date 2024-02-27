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

import static io.streamnative.oxia.client.OxiaClientBuilder.DefaultNamespace;
import static io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientImplBase;
import static io.streamnative.oxia.proto.Status.KEY_NOT_FOUND;
import static io.streamnative.oxia.proto.Status.OK;
import static io.streamnative.oxia.proto.Status.UNEXPECTED_VERSION_ID;
import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.UnexpectedVersionIdException;
import io.streamnative.oxia.client.batch.Batch.ReadBatch;
import io.streamnative.oxia.client.batch.Batch.WriteBatch;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation.SessionInfo;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.BatchMetrics;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.session.Session;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.client.shard.NoShardAvailableException;
import io.streamnative.oxia.proto.DeleteRangeResponse;
import io.streamnative.oxia.proto.DeleteResponse;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.PutResponse;
import io.streamnative.oxia.proto.ReadRequest;
import io.streamnative.oxia.proto.ReadResponse;
import io.streamnative.oxia.proto.WriteRequest;
import io.streamnative.oxia.proto.WriteResponse;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatchTest {
    Function<Long, OxiaStub> clientByShardId;
    @Mock SessionManager sessionManager;
    @Mock Session session;
    long shardId = 1L;
    long sessionId = 1L;
    long startTime = 2L;

    private final OxiaClientImplBase serviceImpl =
            mock(
                    OxiaClientImplBase.class,
                    delegatesTo(
                            new OxiaClientImplBase() {
                                @Override
                                public void write(
                                        WriteRequest request, StreamObserver<WriteResponse> responseObserver) {
                                    writeResponses.forEach(c -> c.accept(responseObserver));
                                }

                                @Override
                                public void read(
                                        ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
                                    readResponses.forEach(c -> c.accept(responseObserver));
                                }
                            }));

    private Server server;
    private OxiaStub stub;
    private final List<Consumer<StreamObserver<WriteResponse>>> writeResponses = new ArrayList<>();
    private final List<Consumer<StreamObserver<ReadResponse>>> readResponses = new ArrayList<>();

    @BeforeEach
    public void setUp() throws Exception {
        writeResponses.clear();
        String serverName = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        stub = new OxiaStub(InProcessChannelBuilder.forName(serverName).directExecutor().build());
        clientByShardId = s -> stub;
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdownNow();
        stub.close();
    }

    @Nested
    @DisplayName("Tests of write batch")
    class WriteBatchTests {
        WriteBatch batch;
        CompletableFuture<PutResult> putCallable = new CompletableFuture<>();
        CompletableFuture<PutResult> putEphemeralCallable = new CompletableFuture<>();
        CompletableFuture<Boolean> deleteCallable = new CompletableFuture<>();
        CompletableFuture<Void> deleteRangeCallable = new CompletableFuture<>();

        PutOperation put = new PutOperation(1L, putCallable, "", new byte[0], Optional.of(1L), false);
        PutOperation putEphemeral =
                new PutOperation(2L, putEphemeralCallable, "", new byte[0], Optional.of(1L), true);
        DeleteOperation delete = new DeleteOperation(3L, deleteCallable, "", Optional.of(1L));
        DeleteRangeOperation deleteRange = new DeleteRangeOperation(4L, deleteRangeCallable, "a", "b");

        String clientIdentifier = "client-id";
        SessionInfo sessionInfo = new SessionInfo(sessionId, clientIdentifier);
        @Mock BatchMetrics.Sample sample;

        @BeforeEach
        void setup() {
            batch =
                    new WriteBatch(
                            clientByShardId,
                            sessionManager,
                            clientIdentifier,
                            shardId,
                            startTime,
                            1024 * 1024,
                            sample);
        }

        @Test
        public void size() {
            batch.add(put);
            assertThat(batch.size()).isEqualTo(1);
            batch.add(delete);
            assertThat(batch.size()).isEqualTo(2);
            batch.add(deleteRange);
            assertThat(batch.size()).isEqualTo(3);
        }

        @Test
        public void add() {
            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);
            assertThat(batch.puts).containsOnly(put);
            assertThat(batch.deletes).containsOnly(delete);
            assertThat(batch.deleteRanges).containsOnly(deleteRange);
        }

        @Test
        public void toProto() {
            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);
            var request = batch.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getPutsList()).containsOnly(put.toProto(Optional.of(sessionInfo)));
                                assertThat(r.getDeletesList()).containsOnly(delete.toProto());
                                assertThat(r.getDeleteRangesList()).containsOnly(deleteRange.toProto());
                            });
        }

        @Test
        public void completeOk() {
            when(session.getSessionId()).thenReturn(sessionId);
            when(sessionManager.getSession(shardId)).thenReturn(session);

            writeResponses.add(
                    o ->
                            o.onNext(
                                    WriteResponse.newBuilder()
                                            .addPuts(PutResponse.newBuilder().setStatus(UNEXPECTED_VERSION_ID).build())
                                            .addPuts(PutResponse.newBuilder().setStatus(OK).build())
                                            .addDeletes(DeleteResponse.newBuilder().setStatus(KEY_NOT_FOUND).build())
                                            .addDeleteRanges(DeleteRangeResponse.newBuilder().setStatus(OK).build())
                                            .build()));
            writeResponses.add(StreamObserver::onCompleted);

            batch.add(put);
            batch.add(putEphemeral);
            batch.add(delete);
            batch.add(deleteRange);

            batch.complete();

            assertThat(putCallable).isCompletedExceptionally();
            assertThat(putEphemeralCallable).isCompleted();
            assertThatThrownBy(putCallable::get)
                    .hasCauseExactlyInstanceOf(UnexpectedVersionIdException.class);
            assertThat(deleteCallable).isCompletedWithValueMatching(r -> !r);
            assertThat(deleteRangeCallable).isCompleted();

            var inOrder = inOrder(sample);
            inOrder.verify(sample).startExec();
            inOrder.verify(sample).stop(null, 0, 4);
        }

        @Test
        public void completeFail() {
            when(session.getSessionId()).thenReturn(sessionId);
            when(sessionManager.getSession(shardId)).thenReturn(session);

            var batchError = new RuntimeException();
            writeResponses.add(o -> o.onError(batchError));

            batch.add(put);
            batch.add(putEphemeral);
            batch.add(delete);
            batch.add(deleteRange);

            batch.complete();

            assertThat(putCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(putEphemeralCallable).isCompletedExceptionally();
            assertThatThrownBy(putEphemeralCallable::get)
                    .hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(deleteCallable).isCompletedExceptionally();
            assertThatThrownBy(deleteCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(deleteRangeCallable).isCompletedExceptionally();
            assertThatThrownBy(deleteRangeCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);

            var inOrder = inOrder(sample);
            inOrder.verify(sample).startExec();
            inOrder.verify(sample).stop(any(StatusRuntimeException.class), eq(0L), eq(4L));
        }

        @Test
        public void completeFailNoClient() {
            batch =
                    new WriteBatch(
                            s -> {
                                throw new NoShardAvailableException(s);
                            },
                            sessionManager,
                            clientIdentifier,
                            shardId,
                            startTime,
                            1024 * 1024,
                            sample);
            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);

            batch.complete();

            assertThat(putCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(NoShardAvailableException.class);
                                assertThat(((NoShardAvailableException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
            assertThat(deleteCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(NoShardAvailableException.class);
                                assertThat(((NoShardAvailableException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
            assertThat(deleteRangeCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(NoShardAvailableException.class);
                                assertThat(((NoShardAvailableException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });

            var inOrder = inOrder(sample);
            inOrder.verify(sample).startExec();
            inOrder.verify(sample).stop(any(NoShardAvailableException.class), eq(0L), eq(3L));
        }

        @Test
        public void shardId() {
            assertThat(batch.getShardId()).isEqualTo(shardId);
        }

        @Test
        public void startTime() {
            assertThat(batch.getStartTime()).isEqualTo(startTime);
        }
    }

    @Nested
    @DisplayName("Tests of read batch")
    class ReadBatchTests {
        ReadBatch batch;
        CompletableFuture<GetResult> getCallable = new CompletableFuture<>();
        GetOperation get = new GetOperation(5L, getCallable, "");
        @Mock BatchMetrics.Sample sample;

        @BeforeEach
        void setup() {
            batch = new ReadBatch(clientByShardId, shardId, startTime, sample);
        }

        @Test
        public void size() {
            batch.add(get);
            assertThat(batch.size()).isEqualTo(1);
        }

        @Test
        public void add() {
            batch.add(get);
            assertThat(batch.gets).containsOnly(get);
        }

        @Test
        public void toProto() {
            batch.add(get);
            var request = batch.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getGetsList()).containsOnly(get.toProto());
                            });
        }

        @Test
        public void completeOk() {
            var getResponse = GetResponse.newBuilder().setStatus(KEY_NOT_FOUND).build();
            readResponses.add(o -> o.onNext(ReadResponse.newBuilder().addGets(getResponse).build()));
            readResponses.add(StreamObserver::onCompleted);

            batch.add(get);
            batch.complete();

            assertThat(getCallable).isCompletedWithValueMatching(Objects::isNull);

            var inOrder = inOrder(sample);
            inOrder.verify(sample).startExec();
            inOrder.verify(sample).stop(null, 0, 1);
        }

        @Test
        public void completeFail() {
            var batchError = new RuntimeException();
            readResponses.add(o -> o.onError(batchError));

            batch.add(get);
            batch.complete();

            assertThat(getCallable).isCompletedExceptionally();
            assertThatThrownBy(getCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);

            var inOrder = inOrder(sample);
            inOrder.verify(sample).startExec();
            inOrder.verify(sample).stop(any(StatusRuntimeException.class), eq(0L), eq(1L));
        }

        @Test
        public void completeFailNoClient() {
            batch =
                    new ReadBatch(
                            s -> {
                                throw new NoShardAvailableException(s);
                            },
                            shardId,
                            startTime,
                            sample);

            batch.add(get);
            batch.complete();

            assertThat(getCallable).isCompletedExceptionally();
            assertThatThrownBy(getCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(NoShardAvailableException.class);
                                assertThat(((NoShardAvailableException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });

            var inOrder = inOrder(sample);
            inOrder.verify(sample).startExec();
            inOrder.verify(sample).stop(any(NoShardAvailableException.class), eq(0L), eq(1L));
        }

        @Test
        public void shardId() {
            assertThat(batch.getShardId()).isEqualTo(shardId);
        }

        @Test
        public void startTime() {
            assertThat(batch.getStartTime()).isEqualTo(startTime);
        }
    }

    @Nested
    @DisplayName("Tests of write batch factory")
    class FactoryTests {
        @Mock Clock clock;
        @Mock BatchMetrics metrics;

        ClientConfig config =
                new ClientConfig(
                        "address",
                        ZERO,
                        ZERO,
                        1,
                        1024 * 1024,
                        0,
                        ZERO,
                        "client_id",
                        Metrics.nullObject,
                        DefaultNamespace);

        @BeforeEach
        void mocking() {
            when(clock.millis()).thenReturn(1L);
        }

        @Nested
        @DisplayName("Tests of write batch factory")
        class WriteBatchFactoryTests {
            @Test
            void apply() {
                var batch =
                        new Batch.WriteBatchFactory(clientByShardId, sessionManager, config, clock, metrics)
                                .apply(shardId);
                assertThat(batch.getStartTime()).isEqualTo(1L);
                assertThat(batch.getShardId()).isEqualTo(shardId);
            }
        }

        @Nested
        @DisplayName("Tests of read batch factory")
        class ReadBatchFactoryTests {
            @Test
            void apply() {
                var batch =
                        new Batch.ReadBatchFactory(clientByShardId, config, clock, metrics).apply(shardId);
                assertThat(batch.getStartTime()).isEqualTo(1L);
                assertThat(batch.getShardId()).isEqualTo(shardId);
            }
        }
    }
}
