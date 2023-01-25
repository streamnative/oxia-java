package io.streamnative.oxia.client.api;


import lombok.NonNull;

/** A notification from an Oxia server indicating a change to a record associated with a key. */
public sealed interface Notification
        permits Notification.KeyCreated, Notification.KeyModified, Notification.KeyDeleted {
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
     * @param version The versionId of the deleted record.
     */
    record KeyDeleted(@NonNull String key, long version) implements Notification {
        public KeyDeleted {
            Version.requireValidVersionId(version);
        }
    }
}
