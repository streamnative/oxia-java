package io.streamnative.oxia.client.shard;


import java.util.function.Predicate;
import lombok.NonNull;

interface ShardStrategy {
    @NonNull
    Predicate<Shard> acceptsKeyPredicate(@NonNull String key);
}
