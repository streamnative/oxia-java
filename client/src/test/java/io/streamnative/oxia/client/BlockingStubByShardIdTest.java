package io.streamnative.oxia.client;

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
        new BlockingStubByShardId(shardManager, factory).apply(1L);
        verify(factory).apply("a");
    }

    @Test
    void noLeader() {
        when(shardManager.leader(1L)).thenThrow(new NoShardAvailableException(1L));
        assertThatThrownBy(() -> new BlockingStubByShardId(shardManager, factory).apply(1L))
                .isInstanceOf(NoShardAvailableException.class);
    }
}
