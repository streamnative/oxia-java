package io.streamnative.oxia.client;


import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class AsyncOxiaClientImpl implements AsyncOxiaClient {
    private final ClientConfig config;

    @Override
    public @NonNull CompletableFuture<PutResult> put(
            @NonNull String key, @NonNull byte[] payload, long expectedVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(@NonNull String key, @NonNull byte[] payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(@NonNull String key, long expectedVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(@NonNull String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(@NonNull String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {}
}
