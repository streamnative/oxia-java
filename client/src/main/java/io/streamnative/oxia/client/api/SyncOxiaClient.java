/*
 * Copyright © 2022-2023 StreamNative Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.oxia.client.api;


import java.util.List;
import lombok.NonNull;

/** Synchronous client for the Oxia service. */
public interface SyncOxiaClient extends AutoCloseable {

    /**
     * Conditionally associates a payload with a key if the server's versionId of the record is as
     * specified, at the instant when the put is applied. The put will not be applied if the server's
     * versionId of the record does not match the expectation set in the call.
     *
     * @param key The key with which the payload should be associated.
     * @param payload The payload to associate with the key.
     * @param expectedVersion The versionId of the record that this put operation is scoped to.
     * @return The result of the put at the specified key.
     * @throws UnexpectedVersionIdException The versionId at the server did not that match supplied in
     *     the call.
     */
    PutResult put(@NonNull String key, byte @NonNull [] payload, long expectedVersion)
            throws UnexpectedVersionIdException;

    /**
     * Associates a payload with a key.
     *
     * @param key The key with which the payload should be associated.
     * @param payload The payload to associate with the key.
     * @return The result of the put at the specified key.
     */
    PutResult put(@NonNull String key, byte @NonNull [] payload);

    /**
     * Conditionally deletes the record associated with the key if the record exists, and the server's
     * versionId of the record is as specified, at the instant when the delete is applied. The delete
     * will not be applied if the server's versionId of the record does not match the expectation set
     * in the call.
     *
     * @param key Deletes the record with the specified key.
     * @param expectedVersion The versionId of the record that this delete operation is scoped to.
     * @return True if the key was actually present on the server, false otherwise.
     * @throws UnexpectedVersionIdException The versionId at the server did not that match supplied in
     *     the call.
     */
    boolean delete(@NonNull String key, long expectedVersion) throws UnexpectedVersionIdException;

    /**
     * Deletes the record associated with the key if the record exists.
     *
     * @param key Deletes the record with the specified key.
     * @return True if the key was actually present on the server, false otherwise.
     */
    boolean delete(@NonNull String key);

    /**
     * Deletes any records with keys within the specified range. TODO describe the ordering of keys
     * within the range, but for now see: <a
     * href="https://github.com/streamnative/oxia/blob/main/server/kv/kv_pebble.go#L468-L499">GitHub</a>.
     *
     * @param minKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param maxKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     */
    void deleteRange(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive);

    /**
     * Returns the record associated with the specified key. The returned value includes the payload,
     * and other metadata.
     *
     * @param key The key associated with the record to be fetched.
     * @return The value associated with the supplied key, or {@code null} if the key did not exist.
     */
    GetResult get(@NonNull String key);

    /**
     * Lists any existing keys within the specified range. TODO describe the ordering of keys within
     * the range, but for now see: <a
     * href="https://github.com/streamnative/oxia/blob/main/server/kv/kv_pebble.go#L468-L499">GitHub</a>.
     *
     * @param minKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param maxKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     * @return The list of keys that exist within the specified range, or an empty list if there were
     *     none.
     */
    @NonNull
    List<String> list(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive);
}
