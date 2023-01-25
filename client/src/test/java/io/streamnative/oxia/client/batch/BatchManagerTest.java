package io.streamnative.oxia.client.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.batch.BatchManager.ShutdownException;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatchManagerTest {

    @Mock Function<Long, Batcher> batcherFactory;
    @Mock Batcher batcher;
    long shardId = 1;
    BatchManager manager;

    @BeforeEach
    void mocking() {
        manager = new BatchManager(batcherFactory);
    }

    @Test
    void computeAbsentBatcher() {
        when(batcherFactory.apply(shardId)).thenReturn(batcher);
        var computed = manager.getBatcher(shardId);
        verify(batcherFactory).apply(shardId);
        assertThat(computed).isSameAs(batcher);
    }

    @Test
    void getPrecomputedBatcher() {
        when(batcherFactory.apply(shardId)).thenReturn(batcher);
        var computed = manager.getBatcher(shardId);
        verify(batcherFactory).apply(shardId);
        var cached = manager.getBatcher(shardId);
        verifyNoMoreInteractions(batcherFactory);
        assertThat(computed).isSameAs(batcher);
        assertThat(cached).isSameAs(batcher);
    }

    @Test
    void getBatcherWhenClosed() throws Exception {
        manager.close();
        assertThatThrownBy(() -> manager.getBatcher(shardId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Batch manager is closed");
    }

    @Test
    void closeClean() throws Exception {
        when(batcherFactory.apply(shardId)).thenReturn(batcher);
        manager.getBatcher(shardId);
        manager.close();
        verify(batcher).close();
    }

    @Test
    void closeDirty() throws Exception {
        var batcherCloseException = new RuntimeException();
        var batcher2 = mock(Batcher.class);
        doThrow(batcherCloseException).when(batcher2).close();
        when(batcherFactory.apply(shardId)).thenReturn(batcher);
        when(batcherFactory.apply(2L)).thenReturn(batcher2);
        manager.getBatcher(shardId);
        manager.getBatcher(2L);
        assertThatThrownBy(() -> manager.close())
                .isInstanceOf(ShutdownException.class)
                .satisfies(
                        e -> {
                            assertThat(((ShutdownException) e).getExceptions())
                                    .containsOnly(batcherCloseException);
                        });
    }
}
