package io.streamnative.oxia.client.shard;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class HashRangeShardStrategyTest {

    private static Stream<Arguments> rangesArgs() {
        return Stream.of(
                Arguments.of(0, 0, false),
                Arguments.of(1, 1, false),
                Arguments.of(1, 2, true),
                Arguments.of(1, 3, true),
                Arguments.of(2, 2, true),
                Arguments.of(2, 3, true),
                Arguments.of(3, 3, false));
    }

    @ParameterizedTest
    @MethodSource("rangesArgs")
    void constantHashFunction(long min, long max, boolean matches) {
        var strategy = new HashRangeShardStrategy(s -> 2L);
        var predicate = strategy.acceptsKeyPredicate("key");
        var shard = new Shard(1, "leader", new HashRange(min, max));
        assertThat(predicate.test(shard)).isEqualTo(matches);
    }
}
