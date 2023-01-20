package io.streamnative.oxia.client.shard;

import static io.streamnative.oxia.client.ProtoUtil.uint32ToLong;

import io.streamnative.oxia.proto.Int32HashRange;
import lombok.NonNull;

record HashRange(long minInclusive, long maxInclusive) {
    HashRange {
        checkHash(minInclusive);
        checkHash(maxInclusive);
        if (maxInclusive < minInclusive) {
            throw new IllegalArgumentException(
                    "Invalid HashRange: [" + minInclusive + ":" + maxInclusive + "]");
        }
    }

    private static void checkHash(long hash) {
        if (hash < 0) {
            throw new IllegalArgumentException("Invalid HashRange bound: " + hash);
        }
    }

    public boolean overlaps(@NonNull HashRange other) {
        return !(minInclusive > other.maxInclusive || maxInclusive < other.minInclusive);
    }

    static @NonNull HashRange fromProto(@NonNull Int32HashRange p) {
        return new HashRange(
                uint32ToLong(p.getMinHashInclusive()), uint32ToLong(p.getMaxHashInclusive()));
    }
}
