package io.streamnative.oxia.client.api;


import lombok.NonNull;

/**
 * Oxia record metadata.
 *
 * @param versionId The current versionId of the record.
 * @param createdTimestamp The instant at which the record was created. In epoch milliseconds.
 * @param modifiedTimestamp The instant at which the record was last updated. In epoch milliseconds.
 */
public record Version(long versionId, long createdTimestamp, long modifiedTimestamp) {
    /** Represents the state where a versionId of a record (and thus the record) does not exist. */
    public static final int VersionIdNotExists = -1;

    public Version {
        requireValidVersionId(versionId);
        requireValidTimestamp(createdTimestamp);
        requireValidTimestamp(modifiedTimestamp);
    }

    /**
     * Checks that the versionId value is either {@link #VersionIdNotExists} or positive.
     *
     * @param versionId The versionId to validate.
     */
    public static void requireValidVersionId(long versionId) {
        if (versionId < VersionIdNotExists) {
            throw new IllegalArgumentException("Invalid version id: " + versionId);
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

    public static @NonNull Version fromProto(@NonNull io.streamnative.oxia.proto.Stat response) {
        return new Version(
                response.getVersion(), response.getCreatedTimestamp(), response.getModifiedTimestamp());
    }
}
