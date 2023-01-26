package io.streamnative.oxia.client.grpc;


import lombok.NonNull;
import lombok.SneakyThrows;

record ServiceAddress(@NonNull String serviceAddress) {

    public @NonNull String host() {
        return serviceAddress.split(":")[0];
    }

    @SneakyThrows
    public int port() {
        return Integer.parseInt(serviceAddress.split(":")[1]);
    }
}
