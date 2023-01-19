package io.streamnative.oxia.client;


import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.UnexpectedVersionException;
import java.util.List;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class SyncOxiaClientImpl implements SyncOxiaClient {

    private final ClientConfig config;

    @Override
    public @NonNull PutResult put(@NonNull String key, @NonNull byte[] payload, long expectedVersion)
            throws UnexpectedVersionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull PutResult put(@NonNull String key, @NonNull byte[] payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(@NonNull String key, long expectedVersion)
            throws UnexpectedVersionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(@NonNull String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRange(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetResult get(@NonNull String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull List<String> list(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {}
}
