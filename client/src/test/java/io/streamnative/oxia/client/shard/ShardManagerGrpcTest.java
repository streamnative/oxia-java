package io.streamnative.oxia.client.shard;

import static io.streamnative.oxia.client.shard.ModelUtil.newShardAssignmentResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientImplBase;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientStub;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import io.streamnative.oxia.proto.ShardAssignmentsResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.awaitility.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShardManagerGrpcTest {

    private final List<Consumer<StreamObserver<ShardAssignmentsResponse>>> responses =
            new ArrayList<>();

    private final OxiaClientImplBase serviceImpl =
            mock(
                    OxiaClientImplBase.class,
                    delegatesTo(
                            new OxiaClientImplBase() {
                                @Override
                                public void shardAssignments(
                                        ShardAssignmentsRequest request,
                                        StreamObserver<ShardAssignmentsResponse> responseObserver) {
                                    responses.forEach(c -> c.accept(responseObserver));
                                }
                            }));

    private Function<String, OxiaClientStub> clientSupplier;
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    public void setUp() throws Exception {
        responses.clear();
        String serverName = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        clientSupplier = a -> OxiaClientGrpc.newStub(channel);
    }

    @AfterEach
    void tearDown() {
        server.shutdownNow();
        channel.shutdownNow();
    }

    @Test
    public void start() throws Exception {
        var gate = new GateResponse();
        responses.add(gate);
        responses.add(o -> o.onNext(newShardAssignmentResponse(1, 2, 3, "leader 1")));
        try (var shardManager = new ShardManager("address", clientSupplier)) {
            var bootstrap = shardManager.start();
            assertThat(bootstrap).isNotCompleted();
            assertThat(shardManager.getAll()).isEmpty();
            gate.open();
            await("bootstrapping").until(bootstrap::isDone);
            assertThat(bootstrap).isNotCompletedExceptionally();
            assertThat(shardManager.getAll()).containsOnly(1);
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
        }
    }

    @Test
    public void update() throws Exception {
        var s1v1 = newShardAssignmentResponse(1, 2, 5, "leader 1");
        var s1v2 = newShardAssignmentResponse(1, 2, 3, "leader 2");
        var strategy = new StaticShardStrategy().assign("key1", s1v1).assign("key2", s1v1);
        var gate = new GateResponse();
        responses.add(o -> o.onNext(s1v1));
        responses.add(gate);
        responses.add(o -> o.onNext(s1v2));
        try (var shardManager = new ShardManager(strategy, "address", clientSupplier)) {
            shardManager.start().get();
            assertThat(shardManager.get("key1")).isEqualTo(1);
            assertThat(shardManager.get("key2")).isEqualTo(1);
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
            strategy.assign("key1", s1v2);
            gate.open();
            await("application of update")
                    .untilAsserted(
                            () -> {
                                assertThat(shardManager.get("key1")).isEqualTo(1);
                                assertThatThrownBy(() -> shardManager.get("key2"))
                                        .isInstanceOf(IllegalStateException.class);
                                assertThat(shardManager.leader(1)).isEqualTo("leader 2");
                            });
            // s1v1 mapped to both k1,k2 -- but s1v2 will only map to k1 -- therefore s1 must have been
            // updated to s1v2
        }
    }

    @Test
    public void overlap() throws Exception {
        var s1 = newShardAssignmentResponse(1, 1, 3, "leader 1");
        var s2 = newShardAssignmentResponse(2, 2, 4, "leader 2");
        var strategy = new StaticShardStrategy().assign("key1", s1);
        var gate = new GateResponse();
        responses.add(o -> o.onNext(s1));
        responses.add(gate);
        responses.add(o -> o.onNext(s2));
        try (var shardManager = new ShardManager(strategy, "address", clientSupplier)) {
            shardManager.start().get();
            assertThat(shardManager.get("key1")).isEqualTo(1);
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
            strategy.assign("key2", s2);
            gate.open();
            await("removal of shard 1")
                    .untilAsserted(
                            () -> {
                                assertThatThrownBy(() -> shardManager.get("key1"))
                                        .isInstanceOf(IllegalStateException.class);
                                assertThatThrownBy(() -> shardManager.leader(1))
                                        .isInstanceOf(IllegalStateException.class);
                                assertThat(shardManager.get("key2")).isEqualTo(2);
                                assertThat(shardManager.leader(2)).isEqualTo("leader 2");
                            }
                            // s1 no longer exists -- it was removed by overlapping s2 -- and thus k1 can no
                            // longer map to it
                            );
        }
    }

    @Test
    public void recoveryFromError() throws Exception {
        var gateToFailure = new GateResponse();
        var gateToRecovery = new GateResponse();
        responses.add(o -> o.onNext(newShardAssignmentResponse(1, 2, 3, "leader 1")));
        responses.add(gateToFailure);
        responses.add(o -> o.onError(new RuntimeException("stream fail")));
        responses.add(gateToRecovery);
        try (var shardManager = new ShardManager("address", clientSupplier)) {
            var bootstrap = shardManager.start();
            await("bootstrapping").timeout(Duration.FIVE_MINUTES).until(bootstrap::isDone);
            assertThat(bootstrap).isNotCompletedExceptionally();
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
            gateToFailure.open();
            responses.clear();
            responses.add(o -> o.onNext(newShardAssignmentResponse(1, 2, 3, "leader 2")));
            gateToRecovery.open();
            await("recovering to leader 2")
                    .timeout(Duration.FIVE_MINUTES)
                    .untilAsserted(() -> assertThat(shardManager.leader(1)).isEqualTo("leader 2"));
        }
    }

    @Test
    public void recoveryFromEndOfStream() throws Exception {
        var gateToFailure = new GateResponse();
        var gateToRecovery = new GateResponse();
        responses.add(o -> o.onNext(newShardAssignmentResponse(1, 2, 3, "leader 1")));
        responses.add(gateToFailure);
        responses.add(StreamObserver::onCompleted);
        responses.add(gateToRecovery);
        try (var shardManager = new ShardManager("address", clientSupplier)) {
            var bootstrap = shardManager.start();
            await("bootstrapping").timeout(Duration.FIVE_MINUTES).until(bootstrap::isDone);
            assertThat(bootstrap).isNotCompletedExceptionally();
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
            gateToFailure.open();
            responses.clear();
            responses.add(o -> o.onNext(newShardAssignmentResponse(1, 2, 3, "leader 2")));
            gateToRecovery.open();
            await("recovering to leader 2")
                    .timeout(Duration.FIVE_MINUTES)
                    .untilAsserted(() -> assertThat(shardManager.leader(1)).isEqualTo("leader 2"));
        }
    }
}
