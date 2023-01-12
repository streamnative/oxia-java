package io.streamnative.oxia.client.api;

import lombok.NonNull;

public interface SyncOxiaClient extends AutoCloseable {
    @NonNull PutResult put(@NonNull String key, @NonNull byte[] payload, long expectedVersion) throws UnexpectedVersionException;
    @NonNull PutResult put(@NonNull String key, @NonNull byte[] payload);
    void delete(@NonNull String key, long expectedVersion) throws KeyNotFoundException, UnexpectedVersionException;
    void delete(@NonNull String key) throws KeyNotFoundException;
    void deleteRange(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive);
    @NonNull GetResult get(@NonNull String key) throws KeyNotFoundException;
    @NonNull ListResult list(@NonNull String minKeyInclusive,@NonNull String maxKeyExclusive);
}
