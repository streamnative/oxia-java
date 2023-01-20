package io.streamnative.oxia.client.shard;

import static java.util.stream.Collectors.toSet;

import io.streamnative.oxia.proto.ShardAssignment;
import java.util.Collection;
import java.util.Set;
import lombok.NonNull;

record Shard(int id, @NonNull String leader, @NonNull HashRange hashRange) {
    public boolean overlaps(@NonNull Shard other) {
        return hashRange.overlaps(other.hashRange);
    }

    public @NonNull Set<Shard> findOverlapping(@NonNull Collection<Shard> other) {
        return other.stream().filter(this::overlaps).collect(toSet());
    }

    static @NonNull Shard fromProto(@NonNull ShardAssignment s) {
        return new Shard(s.getShardId(), s.getLeader(), HashRange.fromProto(s.getInt32HashRange()));
    }
}
