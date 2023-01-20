package io.streamnative.oxia.client;


import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.UnexpectedVersionIdException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class SyncOxiaClientImpl implements SyncOxiaClient {
    private final AsyncOxiaClient asyncClient;

    @SneakyThrows
    @Override
    public @NonNull PutResult put(
            @NonNull String key, byte @NonNull [] payload, long expectedVersionId) {
        try {
            return asyncClient.put(key, payload, expectedVersionId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public @NonNull PutResult put(@NonNull String key, byte @NonNull [] payload) {
        try {
            return asyncClient.put(key, payload).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public boolean delete(@NonNull String key, long expectedVersionId)
            throws UnexpectedVersionIdException {
        try {
            return asyncClient.delete(key, expectedVersionId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public boolean delete(@NonNull String key) {
        try {
            return asyncClient.delete(key).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public void deleteRange(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        try {
            asyncClient.deleteRange(minKeyInclusive, maxKeyExclusive).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public GetResult get(@NonNull String key) {
        try {
            return asyncClient.get(key).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public @NonNull List<String> list(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        try {
            return asyncClient.list(minKeyInclusive, maxKeyExclusive).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Override
    public void close() throws Exception {
        asyncClient.close();
    }
}
