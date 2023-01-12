package io.streamnative.oxia.client.shard;


import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.proto.ShardAssignmentsResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

class GateResponse implements Consumer<StreamObserver<ShardAssignmentsResponse>> {
    private final CompletableFuture<Void> gate = new CompletableFuture<>();

    @Override
    public void accept(
            StreamObserver<ShardAssignmentsResponse> shardAssignmentsResponseStreamObserver) {
        try {
            gate.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void open() {
        gate.complete(null);
    }
}
