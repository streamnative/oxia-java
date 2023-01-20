package io.streamnative.oxia.client.shard;

import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.proto.ShardAssignment;
import io.streamnative.oxia.proto.ShardAssignmentsResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/** Maps keys directly to shards, but exercises the mechanisms of the manager to do so. */
@RequiredArgsConstructor(access = PACKAGE)
class StaticShardStrategy implements ShardStrategy {
    private final Map<String, ShardAssignment> assignments = new HashMap<>();

    @Override
    public @NonNull Predicate<Shard> acceptsKeyPredicate(@NonNull String key) {
        return Optional.ofNullable(assignments.get(key))
                .map(a -> (Predicate<Shard>) s -> isEquivalent(s, a))
                .orElse(s -> false);
    }

    public @NonNull StaticShardStrategy assign(@NonNull String key, @NonNull ShardAssignment shard) {
        assignments.put(key, shard);
        return this;
    }

    public @NonNull StaticShardStrategy assign(
            @NonNull String key, @NonNull ShardAssignmentsResponse response) {
        if (response.getAssignmentsCount() != 1) {
            throw new IllegalArgumentException();
        }
        return assign(key, response.getAssignments(0));
    }

    public @NonNull StaticShardStrategy remove(@NonNull String key) {
        assignments.remove(key);
        return this;
    }

    private static boolean isEquivalent(Shard shard, ShardAssignment assignment) {
        return shard.id() == assignment.getShardId()
                && shard.hashRange().minInclusive() == assignment.getInt32HashRange().getMinHashInclusive()
                && shard.hashRange().maxInclusive() == assignment.getInt32HashRange().getMaxHashInclusive();
    }
}
