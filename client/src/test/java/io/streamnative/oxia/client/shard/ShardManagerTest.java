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
        var assignments = ShardManager.applyUpdates(existing, updates);
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
}
