package io.streamnative.oxia.client.grpc;


import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public interface Receiver extends AutoCloseable {
    @NonNull
    CompletableFuture<Void> receive();

    @NonNull
    CompletableFuture<Void> bootstrap();
}
