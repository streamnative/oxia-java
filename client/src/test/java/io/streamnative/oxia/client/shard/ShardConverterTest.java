package io.streamnative.oxia.client.shard;

import static org.assertj.core.api.Assertions.assertThat;

import io.streamnative.oxia.proto.Int32HashRange;
import io.streamnative.oxia.proto.ShardAssignment;
import org.junit.jupiter.api.Test;

class ShardConverterTest {

    @Test
    void hashRangeFromProto() {
        var hashRange =
                ShardConverter.fromProto(
                        Int32HashRange.newBuilder()
                                .setMinHashInclusive(Integer.MIN_VALUE)
                                .setMaxHashInclusive(-1)
                                .build());
        assertThat(hashRange)
                .satisfies(
                        h -> {
                            assertThat(h.minInclusive()).isEqualTo(Integer.MAX_VALUE + 1L);
                            assertThat(h.maxInclusive()).isEqualTo(4294967295L);
                        });
    }

    @Test
    void shardFromProto() {
        var shard =
                ShardConverter.fromProto(
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

    @Test
    void uint32ToLong() {
        assertThat(ShardConverter.uint32ToLong(1)).isEqualTo(1L);
        assertThat(ShardConverter.uint32ToLong(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
        assertThat(ShardConverter.uint32ToLong(Integer.MIN_VALUE)).isEqualTo(Integer.MAX_VALUE + 1L);
        assertThat(ShardConverter.uint32ToLong(Integer.MIN_VALUE + 1))
                .isEqualTo(Integer.MAX_VALUE + 2L);
        assertThat(ShardConverter.uint32ToLong(-1)).isEqualTo(4294967295L);
        assertThat(ShardConverter.uint32ToLong(0)).isEqualTo(0L);
    }
}
