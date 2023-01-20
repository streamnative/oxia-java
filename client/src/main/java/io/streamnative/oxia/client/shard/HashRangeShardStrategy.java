package io.streamnative.oxia.client.shard;


import java.util.function.Function;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.openhft.hashing.LongHashFunction;

@RequiredArgsConstructor
class HashRangeShardStrategy implements ShardStrategy {

    private final Function<String, Long> hashFn;

    @Override
    @NonNull
    public Predicate<Shard> acceptsKeyPredicate(@NonNull String key) {
        long hash = hashFn.apply(key);
        return shard ->
                shard.hashRange().minInclusive() <= hash && hash <= shard.hashRange().maxInclusive();
    }

    static final Function<String, Long> Xxh332Hash = s -> LongHashFunction.xx3().hashChars(s);

    static final ShardStrategy Xxh332HashRangeShardStrategy = new HashRangeShardStrategy(Xxh332Hash);
}
