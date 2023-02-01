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
package io.streamnative.oxia.client.batch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.batch.Batch.BatchFactory;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatcherTest {

    @Mock Clock clock;
    @Mock BatchFactory batchFactory;
    @Mock Batch batch;
    long shardId = 1L;
    ClientConfig config =
            new ClientConfig("address", n -> {}, Duration.ofMillis(100), Duration.ofMillis(1000), 10, 5);

    BlockingQueue<Operation<?>> queue = new ArrayBlockingQueue<>(config.operationQueueCapacity());
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    Batcher batcher;

    @BeforeEach
    void mocking() {
        batcher = new Batcher(config, shardId, batchFactory, queue, clock);
    }

    @AfterEach
    void teardown() {
        executor.shutdownNow();
    }

    @Test
    void createBatchAndAdd() {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(callback, "key");
        when(batchFactory.apply(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).add(op));
        verify(batch, never()).complete();
    }

    @Test
    void completeBatchOnFull() {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(callback, "key");
        when(batchFactory.apply(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch());
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).complete());
        verifyNoInteractions(clock);
    }

    @Test
    void completeBatchOnFullThenNewBatch() {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(callback, "key");
        when(batchFactory.apply(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch(), 1);
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).complete());
        batcher.add(op);
        await()
                .untilAsserted(
                        () -> {
                            verify(batchFactory, times(2)).apply(shardId);
                            verify(batch, times(2)).add(op);
                        });
        verifyNoInteractions(clock);
    }

    @Test
    void completeBatchOnLingerExpiration() {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(callback, "key");
        when(batchFactory.apply(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.getStartTime()).thenReturn(0L);
        when(clock.millis()).thenReturn(0L, config.batchLinger().toMillis());
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).add(op));
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).complete());
    }

    @Test
    void completeBatchOnLingerExpirationMulti() {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(callback, "key");
        when(batchFactory.apply(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.getStartTime()).thenReturn(0L);
        when(clock.millis()).thenReturn(0L, 100L, 200L, config.batchLinger().toMillis());
        executor.execute(() -> batcher.run());
        batcher.add(op);
        batcher.add(op);
        batcher.add(op);
        await().untilAsserted(() -> verify(batch, times(3)).add(op));
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).complete());
    }

    @Test
    void close() throws Exception {
        Future<?> future = executor.submit(() -> batcher.run());
        assertThat(future).isNotDone();
        batcher.close();
        await().untilAsserted(() -> assertThat(future).isDone());
    }

    @Test
    void interrupt(@Mock BlockingQueue<Operation<?>> queue) throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(callback, "key");
        when(queue.offer(op, config.requestTimeout().toMillis(), MILLISECONDS)).thenReturn(true);
        doReturn(op).when(queue).poll();
        when(queue.poll(anyLong(), eq(MILLISECONDS))).thenThrow(new InterruptedException());
        batcher = new Batcher(config, shardId, batchFactory, queue, clock);
        when(batchFactory.apply(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        batcher.add(op);
        var future = executor.submit(() -> batcher.run());
        await()
                .untilAsserted(
                        () -> {
                            assertThatThrownBy(future::get)
                                    .satisfies(
                                            e -> {
                                                assertThat(e).isInstanceOf(ExecutionException.class);
                                                assertThat(e.getCause())
                                                        .isInstanceOf(RuntimeException.class)
                                                        .hasCauseInstanceOf(InterruptedException.class);
                                            });
                        });
    }

    @Test
    void addTimeout() {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(callback, "key");
        IntStream.range(0, config.operationQueueCapacity()).forEach(i -> batcher.add(op));
        long start = Clock.systemUTC().millis();
        assertThatThrownBy(() -> batcher.add(op)).isInstanceOf(TimeoutException.class);
        assertThat(Clock.systemUTC().millis() - start)
                .isGreaterThanOrEqualTo(config.requestTimeout().toMillis());
    }

    @Test
    void unboundedPollAtStart(@Mock BlockingQueue<Operation<?>> queue) throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(callback, "key");
        when(queue.offer(op, config.requestTimeout().toMillis(), MILLISECONDS)).thenReturn(true);
        doReturn(op).when(queue).poll();
        batcher = new Batcher(config, shardId, batchFactory, queue, clock);
        when(batchFactory.apply(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.getStartTime()).thenReturn(0L);
        when(clock.millis()).thenReturn(0L, 100L, 200L, config.batchLinger().toMillis());
        executor.execute(() -> batcher.run());
        batcher.add(op);
        var inOrder = inOrder(queue);
        await()
                .untilAsserted(
                        () -> {
                            inOrder.verify(queue, atLeastOnce()).poll();
                            inOrder.verify(queue, atLeastOnce()).poll(anyLong(), eq(MILLISECONDS));
                        });
    }
}
