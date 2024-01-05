/*
 * Copyright © 2022-2024 StreamNative Inc.
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

import lombok.NonNull;

/** A notification from an Oxia server indicating a change to a record associated with a key. */
public sealed interface Notification
        permits Notification.KeyCreated, Notification.KeyModified, Notification.KeyDeleted {

    /**
     * @return The key of the record.
     */
    String key();

    /**
     * A record associated with the key has been created.
     *
     * @param key The key of the record created.
     * @param version The versionId of the new record.
     */
    record KeyCreated(@NonNull String key, long version) implements Notification {
        public KeyCreated {
            Version.requireValidVersionId(version);
        }
    }

    /**
     * The record associated with the key has been modified (updated).
     *
     * @param key The key of the record modified.
     * @param version The versionId of the record after the modification.
     */
    record KeyModified(@NonNull String key, long version) implements Notification {
        public KeyModified {
            Version.requireValidVersionId(version);
        }
    }

    /**
     * The record associated with the key has been deleted.
     *
     * @param key The key of the deleted record.
     */
    record KeyDeleted(@NonNull String key) implements Notification {}
}
