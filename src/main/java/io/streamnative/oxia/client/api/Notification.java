package io.streamnative.oxia.client.api;

import lombok.NonNull;

public sealed interface Notification permits Notification.KeyCreated, Notification.KeyModified, Notification.KeyDeleted {
    record KeyCreated(@NonNull String key, long version) implements Notification {
        public KeyCreated {
            Stat.requireValidVersion(version);
        }
    }
    record KeyModified(@NonNull String key, long version) implements Notification {
        public KeyModified {
            Stat.requireValidVersion(version);
        }
    }
    record KeyDeleted(@NonNull String key, long version) implements Notification {
        public KeyDeleted {
            Stat.requireValidVersion(version);
        }
    }
}
