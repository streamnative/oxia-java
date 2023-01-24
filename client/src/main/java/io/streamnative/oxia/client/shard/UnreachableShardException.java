package io.streamnative.oxia.client.shard;


import lombok.Getter;

public class UnreachableShardException extends RuntimeException {
    @Getter private final long shardId;

    public UnreachableShardException(long shardId) {
        super("Unreachable shard: " + shardId);
        this.shardId = shardId;
    }
}
