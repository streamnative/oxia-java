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
package io.streamnative.oxia.client;

import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.shard.NoShardAvailableException;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BlockingStubByShardIdTest {

    @Mock ShardManager shardManager;
    @Mock Function<String, OxiaClientBlockingStub> factory;

    @Test
    void apply() {
        when(shardManager.leader(1L)).thenReturn("a");
        new BlockingStubByShardId(shardManager, factory, clientConfig(false)).apply(1L);
        verify(factory).apply("a");
    }

    @Test
    void noLeader() {
        when(shardManager.leader(1L)).thenThrow(new NoShardAvailableException(1L));
        assertThatThrownBy(
                        () -> new BlockingStubByShardId(shardManager, factory, clientConfig(false)).apply(1L))
                .isInstanceOf(NoShardAvailableException.class);
    }

    @Test
    void standalone() {
        new BlockingStubByShardId(shardManager, factory, clientConfig(true)).apply(1L);
        verify(factory).apply("clientConfigServerAddress");
    }

    private ClientConfig clientConfig(boolean standalone) {
        return new ClientConfig("clientConfigServerAddress", null, ZERO, ZERO, 0, 0, standalone);
    }
}
