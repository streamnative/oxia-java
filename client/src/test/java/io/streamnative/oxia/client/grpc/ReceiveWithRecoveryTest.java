package io.streamnative.oxia.client.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("Receiver recovery tests")
class RecoveryTests {
    @Mock LongFunction<Long> retryIntervalFn;
    @Mock Receiver receiver;
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    CompletableFuture<Void> closed = new CompletableFuture<>();
    CompletableFuture<Void> bootstrap = new CompletableFuture<>();
    CompletableFuture<Void> terminalComplete = new CompletableFuture<>();
    CompletableFuture<Void> terminalError = new CompletableFuture<>();
    CompletableFuture<Void> terminalBlocking = new CompletableFuture<>();
    AtomicLong retryCounter = new AtomicLong();
    ReceiveWithRecovery recovery;

    @BeforeEach
    void mocking() {
        recovery = new ReceiveWithRecovery(executor, closed, retryCounter, retryIntervalFn, receiver);
        terminalComplete.complete(null);
        terminalError.completeExceptionally(new RuntimeException());
    }

    @Test
    void retry() throws Exception {
        //noinspection unchecked
        when(receiver.receive())
                .thenReturn(terminalError, terminalError, terminalComplete, terminalBlocking);
        when(retryIntervalFn.apply(anyLong())).thenReturn(1L);
        var terminated = recovery.receive();
        await().untilAsserted(() -> assertThat(retryCounter).hasValue(4));
        recovery.close();
        await().until(terminated::isDone);
        verify(receiver, times(4)).receive();
        var inOrder = inOrder(retryIntervalFn);
        inOrder.verify(retryIntervalFn).apply(1);
        inOrder.verify(retryIntervalFn).apply(2);
        inOrder.verify(retryIntervalFn).apply(3);
    }

    @Test
    void close() throws Exception {
        recovery.close();
        assertThat(executor.isShutdown()).isTrue();
        assertThat(closed).isCompleted();
    }

    @Test
    void bootstrap() {
        when(receiver.bootstrap()).thenReturn(bootstrap);
        assertThat(recovery.bootstrap()).isEqualTo(bootstrap);
    }
}
