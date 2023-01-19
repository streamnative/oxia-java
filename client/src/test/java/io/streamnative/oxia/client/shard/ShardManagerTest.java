package io.streamnative.oxia.client.shard;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ShardManagerTest {

    @Test
    void applyUpdates() {
        var existing =
                Map.of(
                        1, new Shard(1, "leader 1", new HashRange(1, 3)),
                        2, new Shard(2, "leader 1", new HashRange(7, 9)),
                        3, new Shard(3, "leader 2", new HashRange(4, 6)),
                        4, new Shard(4, "leader 2", new HashRange(10, 11)),
                        5, new Shard(5, "leader 3", new HashRange(11, 12)),
                        6, new Shard(6, "leader 4", new HashRange(13, 13)));
        var updates =
                List.of(
                        new Shard(1, "leader 4", new HashRange(1, 3)), // Leader change
                        new Shard(2, "leader 4", new HashRange(7, 9)), //
                        new Shard(3, "leader 2", new HashRange(4, 5)), // Split
                        new Shard(7, "leader 3", new HashRange(6, 6)), //
                        new Shard(4, "leader 2", new HashRange(10, 12)) // Merge
                        );
        var assignments = ShardManager.Assignments.applyUpdates(existing, updates);
        assertThat(assignments)
                .satisfies(
                        a -> {
                            assertThat(a)
                                    .containsOnly(
                                            entry(1, new Shard(1, "leader 4", new HashRange(1, 3))),
                                            entry(2, new Shard(2, "leader 4", new HashRange(7, 9))),
                                            entry(3, new Shard(3, "leader 2", new HashRange(4, 5))),
                                            entry(7, new Shard(7, "leader 3", new HashRange(6, 6))),
                                            entry(4, new Shard(4, "leader 2", new HashRange(10, 12))),
                                            entry(6, new Shard(6, "leader 4", new HashRange(13, 13))));
                            assertThat(a).isUnmodifiable();
                        });
    }

    @Test
    void exponentialBackOffFn() {
        var fn = new ShardManager.ReceiveWithRecovery.ExponentialBackoff(() -> 10L, 100L, 1000L);
        assertThat(fn.apply(1)).isEqualTo(12L);
        assertThat(fn.apply(2)).isEqualTo(14L);
        assertThat(fn.apply(3)).isEqualTo(18L);
        assertThat(fn.apply(4)).isEqualTo(26L);
        assertThat(fn.apply(5)).isEqualTo(42L);
        assertThat(fn.apply(6)).isEqualTo(74L);
        assertThat(fn.apply(7)).isEqualTo(138L);
        assertThat(fn.apply(8)).isEqualTo(266L);
        assertThat(fn.apply(9)).isEqualTo(522L);
        assertThat(fn.apply(10)).isEqualTo(1000L);
    }
}
