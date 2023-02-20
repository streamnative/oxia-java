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
import java.util.function.Consumer;
import lombok.NonNull;

/** Synchronous client for the Oxia service. */
public interface SyncOxiaClient extends AutoCloseable {

    /**
     * Conditionally associates a value with a key if the server's versionId of the record is as
     * specified, at the instant when the put is applied. The put will not be applied if the server's
     * versionId of the record does not match the expectation set in the call. If you wish the put to
     * succeed only if the key does not already exist on the server, then pass the {@link
     * Version#KeyNotExists} value.
     *
     * @param key The key with which the value should be associated.
     * @param value The value to associate with the key.
     * @param options Set {@link PutOption options} for the put.
     * @return The result of the put at the specified key.
     * @throws UnexpectedVersionIdException The versionId at the server did not that match supplied in
     *     the call.
     */
    PutResult put(@NonNull String key, byte @NonNull [] value, PutOption... options)
            throws UnexpectedVersionIdException;

    /**
     * Conditionally deletes the record associated with the key if the record exists, and the server's
     * versionId of the record is as specified, at the instant when the delete is applied. The delete
     * will not be applied if the server's versionId of the record does not match the expectation set
     * in the call.
     *
     * @param key Deletes the record with the specified key.
     * @param options Set {@link DeleteOption options} for the delete.
     * @return True if the key was actually present on the server, false otherwise.
     * @throws UnexpectedVersionIdException The versionId at the server did not that match supplied in
     *     the call.
     */
    boolean delete(@NonNull String key, @NonNull DeleteOption... options)
            throws UnexpectedVersionIdException;

    /**
     * Deletes any records with keys within the specified range. For more information on how keys are
     * sorted, check the relevant section in the <a
     * href="https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md">Oxia
     * documentation</a>.
     *
     * @param minKeyInclusive The key that declares start of the range, and is <b>included</b> from
     *     the range.
     * @param maxKeyExclusive The key that declares the end of the range, and is <b>excluded</b> from
     *     the range.
     */
    void deleteRange(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive);

    /**
     * Returns the record associated with the specified key. The returned value includes the value,
     * and other metadata.
     *
     * @param key The key associated with the record to be fetched.
     * @return The value associated with the supplied key, or {@code null} if the key did not exist.
     */
    GetResult get(@NonNull String key);

    /**
     * Lists any existing keys within the specified range. For more information on how keys are
     * sorted, check the relevant section in the <a
     * href="https://github.com/streamnative/oxia/blob/main/docs/oxia-key-sorting.md">Oxia
     * documentation</a>.
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

    /**
     * Registers a callback to receive Oxia {@link Notification record change notifications}. Multiple
     * callbacks can be registered.
     *
     * @param notificationCallback A callback to receive notifications.
     */
    void notifications(@NonNull Consumer<Notification> notificationCallback);
}
