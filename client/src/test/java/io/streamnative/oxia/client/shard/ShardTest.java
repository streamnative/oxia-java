package io.streamnative.oxia.client.shard;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamnative.oxia.proto.Int32HashRange;
import io.streamnative.oxia.proto.ShardAssignment;
import org.junit.jupiter.api.Test;

class ShardTest {

    @Test
    void shardFromProto() {
        var shard =
                Shard.fromProto(
                        ShardAssignment.newBuilder()
                                .setShardId(1)
                                .setInt32HashRange(
                                        Int32HashRange.newBuilder()
                                                .setMinHashInclusive(2)
                                                .setMaxHashInclusive(3)
                                                .build())
                                .build());
        assertThat(shard)
                .satisfies(
                        s -> {
                            assertThat(s.id()).isEqualTo(1);
                            assertThat(s.hashRange())
                                    .satisfies(
                                            h -> {
                                                assertThat(h.minInclusive()).isEqualTo(2L);
                                                assertThat(h.maxInclusive()).isEqualTo(3L);
                                            });
                        });
    }
}
