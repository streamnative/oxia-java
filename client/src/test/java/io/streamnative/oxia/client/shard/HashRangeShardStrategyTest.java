/*
 * Copyright © 2022-2023 StreamNative Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.oxia.client.shard;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomUtils;

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

    private static Stream<Arguments> xxh332Args() {
        return Stream.of(
                Arguments.of("foo", 125730186L),
                Arguments.of("bar", 2687685474L),
                Arguments.of("baz", 862947621L)
        );
    }

    @ParameterizedTest
    @MethodSource("xxh332Args")
    void xxh332Test(String key, long expected) {
        var hash = HashRangeShardStrategy.Xxh332Hash.apply(key);
        assertThat(hash).isEqualTo(expected);
    }
}
