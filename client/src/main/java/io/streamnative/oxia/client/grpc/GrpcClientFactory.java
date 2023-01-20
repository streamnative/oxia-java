package io.streamnative.oxia.client.grpc;


import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import java.util.function.Function;

record GrpcClientFactory(ClientConfig clientConfig) {

    public Function<String, OxiaClientGrpc.OxiaClientBlockingStub> blockingClient(
            String serviceHost) {
        return null;
    }

    public Function<String, OxiaClientGrpc.OxiaClientBlockingStub> nonBlockingClient(
            String serviceHost) {
        return null;
    }
}
