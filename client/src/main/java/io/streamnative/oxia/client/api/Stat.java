package io.streamnative.oxia.client.api;

/**
 * Oxia record metadata.
 *
 * @param version The current version of the record.
 * @param createdTimestamp The instant at which the record was created. In epoch milliseconds.
 * @param modifiedTimestamp The instant at which the record was last updated. In epoch milliseconds.
 */
public record Stat(long version, long createdTimestamp, long modifiedTimestamp) {
    /** Represents the state where a version of a record (and thus the record) does not exist. */
    public static final int VersionNotExists = -1;

    public Stat {
        requireValidVersion(version);
        requireValidTimestamp(createdTimestamp);
        requireValidTimestamp(modifiedTimestamp);
    }

    /**
     * Checks that the version value is either {@link #VersionNotExists} or positive.
     *
     * @param version The version to validate.
     */
    public static void requireValidVersion(long version) {
        if (version < VersionNotExists) {
            throw new IllegalArgumentException("Invalid version: " + version);
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
}
