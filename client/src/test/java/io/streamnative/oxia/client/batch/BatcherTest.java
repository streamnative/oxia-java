/*
 * Copyright © 2022-2024 StreamNative Inc.
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

import static io.streamnative.oxia.client.OxiaClientBuilder.DefaultNamespace;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatcherTest {

    @Mock BatchFactory batchFactory;
    @Mock Batch batch;
    long shardId = 1L;
    ClientConfig config =
            new ClientConfig(
                    "address",
                    Duration.ofMillis(100),
                    Duration.ofMillis(1000),
                    10,
                    1024 * 1024,
                    0,
                    Duration.ofMillis(1000),
                    "client_id",
                    null,
                    DefaultNamespace);

    BlockingQueue<Operation<?>> queue = new LinkedBlockingDeque<>();
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    Batcher batcher;

    @BeforeEach
    void mocking() {
        batcher = new Batcher(config, shardId, batchFactory, queue);
    }

    @AfterEach
    void teardown() {
        executor.shutdownNow();
    }

    @Test
    void createBatchAndAdd() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(1L, callback, "key");
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.canAdd(any())).thenReturn(true);
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).add(op));
        verify(batch, never()).send();
    }

    @Test
    void sendBatchOnFull() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(1L, callback, "key");
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch());
        when(batch.canAdd(any())).thenReturn(true);
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).send());
    }

    @Test
    void addWhenNextDoesNotFit() {
        var callback = new CompletableFuture<PutResult>();
        Operation<?> op =
                new Operation.WriteOperation.PutOperation(
                        1L, callback, "key", "value".getBytes(StandardCharsets.UTF_8), Optional.empty(), false);
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch(), 1);
        when(batch.canAdd(any())).thenReturn(false);
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await()
                .untilAsserted(
                        () -> {
                            verify(batchFactory, times(2)).getBatch(shardId);
                            verify(batch).add(op);
                        });
    }

    @Test
    void sendBatchOnFullThenNewBatch() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(1L, callback, "key");
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(config.maxRequestsPerBatch(), 1);
        when(batch.canAdd(any())).thenReturn(true);
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).send());
        batcher.add(op);
        await()
                .untilAsserted(
                        () -> {
                            verify(batchFactory, times(2)).getBatch(shardId);
                            verify(batch, times(2)).add(op);
                        });
    }

    @Test
    void sendBatchOnLingerExpiration() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(1L, callback, "key");
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.canAdd(any())).thenReturn(true);
        executor.execute(() -> batcher.run());
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).add(op));
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).send());
    }

    @Test
    void sendBatchOnLingerExpirationMulti() throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(1L, callback, "key");
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        when(batch.canAdd(any())).thenReturn(true);
        when(batch.getStartTimeNanos()).thenReturn(System.nanoTime());
        executor.execute(() -> batcher.run());
        batcher.add(op);
        batcher.add(op);
        batcher.add(op);
        await().untilAsserted(() -> verify(batch, times(3)).add(op));
        batcher.add(op);
        await().untilAsserted(() -> verify(batch).send());
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
        Operation<?> op = new GetOperation(1L, callback, "key");
        doReturn(op).when(queue).take();
        when(queue.poll(anyLong(), eq(NANOSECONDS))).thenThrow(new InterruptedException());
        batcher = new Batcher(config, shardId, batchFactory, queue);
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
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
    void unboundedTakeAtStart(@Mock BlockingQueue<Operation<?>> queue) throws Exception {
        var callback = new CompletableFuture<GetResult>();
        Operation<?> op = new GetOperation(1L, callback, "key");
        doReturn(op).when(queue).take();
        batcher = new Batcher(config, shardId, batchFactory, queue);
        when(batchFactory.getBatch(shardId)).thenReturn(batch);
        when(batch.size()).thenReturn(1);
        executor.execute(() -> batcher.run());
        batcher.add(op);
        var inOrder = inOrder(queue);
        await()
                .untilAsserted(
                        () -> {
                            inOrder.verify(queue, atLeastOnce()).take();
                            inOrder.verify(queue, atLeastOnce()).poll(anyLong(), eq(NANOSECONDS));
                        });
    }
}
