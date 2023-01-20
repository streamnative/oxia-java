package io.streamnative.oxia.client.shard;


import io.streamnative.oxia.proto.Int32HashRange;
import io.streamnative.oxia.proto.ShardAssignment;
import io.streamnative.oxia.proto.ShardAssignmentsResponse;
import lombok.NonNull;

public class ModelFactory {
    static @NonNull Int32HashRange newHashRange(int min, int max) {
        return Int32HashRange.newBuilder().setMinHashInclusive(min).setMaxHashInclusive(max).build();
    }

    static @NonNull ShardAssignment newShardAssignment(
            int id, int min, int max, @NonNull String leader) {
        return ShardAssignment.newBuilder()
                .setShardId(id)
                .setLeader(leader)
                .setInt32HashRange(newHashRange(min, max))
                .build();
    }

    static @NonNull ShardAssignmentsResponse newShardAssignmentResponse(
            int id, int min, int max, @NonNull String leader) {
        return ShardAssignmentsResponse.newBuilder()
                .addAssignments(newShardAssignment(id, min, max, leader))
                .build();
    }
}
