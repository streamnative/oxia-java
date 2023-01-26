package io.streamnative.oxia.client;


import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
import java.util.function.Function;
import lombok.NonNull;

record BlockingStubByShardId(
        @NonNull ShardManager shardManager,
        @NonNull Function<String, OxiaClientBlockingStub> blockingStubFactory)
        implements Function<Long, OxiaClientBlockingStub> {
    @Override
    public @NonNull OxiaClientBlockingStub apply(@NonNull Long shardId) {
        return blockingStubFactory.apply(shardManager.leader(shardId));
    }
}
