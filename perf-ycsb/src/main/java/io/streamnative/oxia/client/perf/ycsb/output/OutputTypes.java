package io.streamnative.oxia.client.perf.ycsb.output;

import static java.util.Objects.requireNonNull;

import java.util.Locale;

public enum OutputTypes {
    LOG,
    PULSAR;

    public static OutputTypes fromString(String type) {
        requireNonNull(type);
        for (OutputTypes gType : OutputTypes.values()) {
            if (gType.name().toLowerCase(Locale.ROOT).equals(type.toLowerCase(Locale.ROOT))) {
                return gType;
            }
        }
        throw new IllegalArgumentException("unknown generator type:" + type);
    }
}
