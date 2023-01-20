package io.streamnative.oxia.client.shard;

import static io.streamnative.oxia.client.shard.HashRangeShardStrategy.Xxh332Hash;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

// This test should be used to verify cross-platform consistency
public class Xxh332HashTest {

    private static Stream<Arguments> hashArgs() {
        return Stream.of(
                Arguments.of("oxia metadata service", 5404817113047210190L),
                Arguments.of("a", 970730967732026843L));
    }

    @ParameterizedTest
    @MethodSource("hashArgs")
    void ranges(String key, long hash) {
        assertThat(Xxh332Hash.apply(key)).isEqualTo(hash);
    }
}
