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

import java.util.Optional;
import lombok.NonNull;

/**
 * Oxia record metadata.
 *
 * @param versionId The current versionId of the record.
 * @param createdTimestamp The instant at which the record was created. In epoch milliseconds.
 * @param modifiedTimestamp The instant at which the record was last updated. In epoch milliseconds.
 * @param modificationsCount The number of modifications since the record was last created.
 * @param sessionId The session to which this record is scoped.
 * @param clientIdentifier The client to which this record was scoped.
 */
public record Version(
        long versionId,
        long createdTimestamp,
        long modifiedTimestamp,
        long modificationsCount,
        @NonNull Optional<Long> sessionId,
        @NonNull Optional<String> clientIdentifier) {
    public static final long KeyNotExists = -1;

    /** Represents the state where a versionId of a record (and thus the record) does not exist. */
    public Version {
        requireValidVersionId(versionId);
        requireValidTimestamp(createdTimestamp);
        requireValidTimestamp(modifiedTimestamp);
        requireValidModificationsCount(modificationsCount);
    }

    /**
     * Checks that the versionId value is non-negative.
     *
     * @param versionId The versionId to validate.
     */
    public static void requireValidVersionId(long versionId) {
        if (versionId < 0) {
            throw new IllegalArgumentException("Invalid versionId: " + versionId);
        }
    }

    /**
     * Checks that the timestamp is valid (positive).
     *
     * @param timestamp The timestamp to validate.
     */
    public static void requireValidTimestamp(long timestamp) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
        }
    }

    /**
     * Checks that the modificationsCount value is non-negative.
     *
     * @param modificationsCount The modificationsCount to validate.
     */
    public static void requireValidModificationsCount(long modificationsCount) {
        if (modificationsCount < 0) {
            throw new IllegalArgumentException("Invalid modificationsCount: " + modificationsCount);
        }
    }

}
