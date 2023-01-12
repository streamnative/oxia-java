package io.streamnative.oxia.client.api;

import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public interface AsyncOxiaClient extends AutoCloseable {
    /**
     * The returned future my complete exceptionally with an {@link UnexpectedVersionException}.
     */
    @NonNull CompletableFuture<PutResult> put(@NonNull String key, @NonNull byte[] payload, long expectedVersion);

    @NonNull CompletableFuture<PutResult> put(@NonNull String key, @NonNull byte[] payload);

    /**
     * The returned future my complete exceptionally with an {@link UnexpectedVersionException} or a
     * {@link KeyNotFoundException}.
     */
    @NonNull CompletableFuture<Void> delete(@NonNull String key, long expectedVersion);

    /**
     * The returned future my complete exceptionally with a {@link KeyNotFoundException}.
     */
    @NonNull CompletableFuture<Void> delete(@NonNull String key);

    @NonNull CompletableFuture<Void> deleteRange(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive);

    /**
     * The returned future my complete exceptionally with a {@link KeyNotFoundException}.
     */
    @NonNull CompletableFuture<GetResult> get(@NonNull String key);

    @NonNull CompletableFuture<ListResult> list(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive);
}
