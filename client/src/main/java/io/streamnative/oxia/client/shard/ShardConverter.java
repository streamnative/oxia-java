package io.streamnative.oxia.client.shard;


import io.streamnative.oxia.proto.Int32HashRange;
import io.streamnative.oxia.proto.ShardAssignment;
import java.nio.ByteBuffer;
import lombok.NonNull;

public class ShardConverter {

    static @NonNull HashRange fromProto(@NonNull Int32HashRange p) {
        return new HashRange(
                uint32ToLong(p.getMinHashInclusive()), uint32ToLong(p.getMaxHashInclusive()));
    }

    static @NonNull Shard fromProto(@NonNull ShardAssignment s) {
        return new Shard(s.getShardId(), s.getLeader(), fromProto(s.getInt32HashRange()));
    }

    static long uint32ToLong(int unit32AsInt) {
        return ByteBuffer.allocate(8).putInt(0).putInt(unit32AsInt).flip().getLong();
    }
}
