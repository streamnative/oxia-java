package io.streamnative.oxia.client.api;

public record Stat(long version, long createdTimestamp, long modifiedTimestamp) {
    public static final int VersionNotExists = -1;

    public Stat {
        requireValidVersion(version);
        requireValidTimestamp(createdTimestamp);
        requireValidTimestamp(modifiedTimestamp);
    }

    public static void requireValidVersion(long version) {
        if (version < VersionNotExists) {
            throw new IllegalArgumentException("Invalid version: " + version);
        }
    }

    public static void requireValidTimestamp(long timestamp) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
        }
    }
}
