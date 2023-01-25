package io.streamnative.oxia.client.batch;

import static io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
import static io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientImplBase;
import static io.streamnative.oxia.proto.OxiaClientGrpc.newBlockingStub;
import static io.streamnative.oxia.proto.Status.KEY_NOT_FOUND;
import static io.streamnative.oxia.proto.Status.OK;
import static io.streamnative.oxia.proto.Status.UNEXPECTED_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.KeyNotFoundException;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.UnexpectedVersionException;
import io.streamnative.oxia.client.batch.Batch.ReadBatch;
import io.streamnative.oxia.client.batch.Batch.WriteBatch;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.shard.UnreachableShardException;
import io.streamnative.oxia.proto.DeleteRangeResponse;
import io.streamnative.oxia.proto.DeleteResponse;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.ListResponse;
import io.streamnative.oxia.proto.PutResponse;
import io.streamnative.oxia.proto.ReadRequest;
import io.streamnative.oxia.proto.ReadResponse;
import io.streamnative.oxia.proto.WriteRequest;
import io.streamnative.oxia.proto.WriteResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

    Function<Long, Optional<OxiaClientBlockingStub>> clientByShardId;
    long shardId = 1L;
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
    private ManagedChannel channel;
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
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        clientByShardId = s -> Optional.of(newBlockingStub(channel));
    }

    @AfterEach
    void tearDown() {
        server.shutdownNow();
        channel.shutdownNow();
    }

    @Nested
    @DisplayName("Tests of write batch")
    class WriteBatchTests {
        WriteBatch batch;
        CompletableFuture<PutResult> putCallable = new CompletableFuture<>();
        CompletableFuture<Boolean> deleteCallable = new CompletableFuture<>();
        CompletableFuture<Void> deleteRangeCallable = new CompletableFuture<>();

        PutOperation put = new PutOperation(putCallable, "", new byte[0], 1L);
        DeleteOperation delete = new DeleteOperation(deleteCallable, "", 1L);
        DeleteRangeOperation deleteRange = new DeleteRangeOperation(deleteRangeCallable, "a", "b");

        @BeforeEach
        void setup() {
            batch = new WriteBatch(clientByShardId, shardId, startTime);
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
                                assertThat(r.getPutsList()).containsOnly(put.toProto());
                                assertThat(r.getDeletesList()).containsOnly(delete.toProto());
                                assertThat(r.getDeleteRangesList()).containsOnly(deleteRange.toProto());
                            });
        }

        @Test
        public void completeOk() {
            writeResponses.add(
                    o ->
                            o.onNext(
                                    WriteResponse.newBuilder()
                                            .addPuts(PutResponse.newBuilder().setStatus(UNEXPECTED_VERSION).build())
                                            .addDeletes(DeleteResponse.newBuilder().setStatus(KEY_NOT_FOUND).build())
                                            .addDeleteRanges(DeleteRangeResponse.newBuilder().setStatus(OK).build())
                                            .build()));
            writeResponses.add(StreamObserver::onCompleted);

            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);

            batch.complete();

            assertThat(putCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .hasCauseExactlyInstanceOf(UnexpectedVersionException.class);
            assertThat(deleteCallable).isCompletedWithValueMatching(r -> !r);
            assertThat(deleteRangeCallable).isCompleted();
        }

        @Test
        public void completeFail() {
            var batchError = new RuntimeException();
            writeResponses.add(o -> o.onError(batchError));

            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);

            batch.complete();

            assertThat(putCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(deleteCallable).isCompletedExceptionally();
            assertThatThrownBy(deleteCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(deleteRangeCallable).isCompletedExceptionally();
            assertThatThrownBy(deleteRangeCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
        }

        @Test
        public void completeFailNoClient() {
            batch = new WriteBatch(s -> Optional.empty(), shardId, startTime);

            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);

            batch.complete();

            assertThat(putCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(UnreachableShardException.class);
                                assertThat(((UnreachableShardException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
            assertThat(deleteCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(UnreachableShardException.class);
                                assertThat(((UnreachableShardException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
            assertThat(deleteRangeCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(UnreachableShardException.class);
                                assertThat(((UnreachableShardException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
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
        CompletableFuture<List<String>> listCallable = new CompletableFuture<>();

        GetOperation get = new GetOperation(getCallable, "");
        ListOperation list = new ListOperation(listCallable, "a", "b");

        @BeforeEach
        void setup() {
            batch = new ReadBatch(clientByShardId, shardId, startTime);
        }

        @Test
        public void size() {
            batch.add(get);
            assertThat(batch.size()).isEqualTo(1);
            batch.add(list);
            assertThat(batch.size()).isEqualTo(2);
        }

        @Test
        public void add() {
            batch.add(get);
            batch.add(list);
            assertThat(batch.gets).containsOnly(get);
            assertThat(batch.lists).containsOnly(list);
        }

        @Test
        public void toProto() {
            batch.add(get);
            batch.add(list);
            var request = batch.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getGetsList()).containsOnly(get.toProto());
                                assertThat(r.getListsList()).containsOnly(list.toProto());
                            });
        }

        @Test
        public void completeOk() {
            var getResponse = GetResponse.newBuilder().setStatus(KEY_NOT_FOUND).build();
            var listResponse = ListResponse.newBuilder().addAllKeys(List.of("a", "b", "c")).build();
            readResponses.add(
                    o ->
                            o.onNext(
                                    ReadResponse.newBuilder().addGets(getResponse).addLists(listResponse).build()));
            readResponses.add(StreamObserver::onCompleted);

            batch.add(get);
            batch.add(list);

            batch.complete();

            assertThatThrownBy(getCallable::get).hasCauseExactlyInstanceOf(KeyNotFoundException.class);
            assertThat(listCallable).isCompletedWithValueMatching(r -> r.equals(List.of("a", "b", "c")));
        }

        @Test
        public void completeFail() {
            var batchError = new RuntimeException();
            readResponses.add(o -> o.onError(batchError));

            batch.add(get);
            batch.add(list);

            batch.complete();

            assertThat(getCallable).isCompletedExceptionally();
            assertThatThrownBy(getCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(listCallable).isCompletedExceptionally();
            assertThatThrownBy(listCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
        }

        @Test
        public void completeFailNoClient() {
            batch = new ReadBatch(s -> Optional.empty(), shardId, startTime);

            batch.add(get);
            batch.add(list);

            batch.complete();

            assertThat(getCallable).isCompletedExceptionally();
            assertThatThrownBy(getCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(UnreachableShardException.class);
                                assertThat(((UnreachableShardException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
            assertThat(listCallable).isCompletedExceptionally();
            assertThatThrownBy(listCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(UnreachableShardException.class);
                                assertThat(((UnreachableShardException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
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

        ClientConfig config = new ClientConfig("address", n -> {}, Duration.ZERO, Duration.ZERO, 1, 1);

        @BeforeEach
        void mocking() {
            when(clock.millis()).thenReturn(1L);
        }

        @Nested
        @DisplayName("Tests of write batch factory")
        class WriteBatchFactoryTests {

            @Test
            void apply() {
                var batch = new Batch.WriteBatchFactory(clientByShardId, config, clock).apply(shardId);
                assertThat(batch.getStartTime()).isEqualTo(1L);
                assertThat(batch.getShardId()).isEqualTo(shardId);
            }
        }

        @Nested
        @DisplayName("Tests of read batch factory")
        class ReadBatchFactoryTests {
            @Test
            void apply() {
                var batch = new Batch.ReadBatchFactory(clientByShardId, config, clock).apply(shardId);
                assertThat(batch.getStartTime()).isEqualTo(1L);
                assertThat(batch.getShardId()).isEqualTo(shardId);
            }
        }
    }
}
