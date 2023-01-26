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
