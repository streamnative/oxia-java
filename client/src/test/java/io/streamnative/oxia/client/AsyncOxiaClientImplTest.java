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

import static io.streamnative.oxia.client.ProtoUtil.VersionIdNotSpecified;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.batch.BatchManager;
import io.streamnative.oxia.client.batch.Batcher;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.ListOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.grpc.ChannelManager;
import io.streamnative.oxia.client.notify.NotificationManager;
import io.streamnative.oxia.client.shard.ShardManager;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AsyncOxiaClientImplTest {

    @Mock ChannelManager channelManager;
    @Mock ShardManager shardManager;
    @Mock NotificationManager notificationManager;
    @Mock BatchManager readBatchManager;
    @Mock BatchManager writeBatchManager;
    @Mock Batcher batcher;

    AsyncOxiaClientImpl client;

    @BeforeEach
    void setUp() {
        client =
                new AsyncOxiaClientImpl(
                        channelManager, shardManager, notificationManager, readBatchManager, writeBatchManager);
    }

    @Test
    void put() {
        var opCaptor = ArgumentCaptor.forClass(PutOperation.class);
        var shardId = 1L;
        var key = "key";
        var value = "hello".getBytes(UTF_8);
        when(shardManager.get(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.put(key, value);
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).isEqualTo(VersionIdNotSpecified);
                            assertThat(o.value()).isEqualTo(value);
                            assertThat(o.callback()).isSameAs(result);
                        });
    }

    @Test
    void putExpectedVersion() {
        var opCaptor = ArgumentCaptor.forClass(PutOperation.class);
        var shardId = 1L;
        var key = "key";
        var expectedVersionId = 2L;
        var value = "hello".getBytes(UTF_8);
        when(shardManager.get(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.put(key, value, expectedVersionId);
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).isEqualTo(expectedVersionId);
                            assertThat(o.value()).isEqualTo(value);
                            assertThat(o.callback()).isSameAs(result);
                        });
    }

    @Test
    void delete() {
        var opCaptor = ArgumentCaptor.forClass(DeleteOperation.class);
        var shardId = 1L;
        var key = "key";
        when(shardManager.get(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.delete(key);
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).isEqualTo(VersionIdNotSpecified);
                            assertThat(o.callback()).isSameAs(result);
                        });
    }

    @Test
    void deleteExpectedVersion() {
        var opCaptor = ArgumentCaptor.forClass(DeleteOperation.class);
        var shardId = 1L;
        var key = "key";
        var expectedVersionId = 2L;
        when(shardManager.get(key)).thenReturn(shardId);
        when(writeBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.delete(key, expectedVersionId);
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).isEqualTo(expectedVersionId);
                            assertThat(o.callback()).isSameAs(result);
                        });
    }

    @Test
    void deleteRange() {
        var batcher1 = mock(Batcher.class);
        var batcher2 = mock(Batcher.class);
        var batcher3 = mock(Batcher.class);
        var opCaptor1 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var opCaptor2 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var opCaptor3 = ArgumentCaptor.forClass(DeleteRangeOperation.class);
        var min = "a-min";
        var max = "z-max";
        when(shardManager.getAll()).thenReturn(List.of(1L, 2L, 3L));
        when(writeBatchManager.getBatcher(1L)).thenReturn(batcher1);
        when(writeBatchManager.getBatcher(2L)).thenReturn(batcher2);
        when(writeBatchManager.getBatcher(3L)).thenReturn(batcher3);
        doNothing().when(batcher1).add(opCaptor1.capture());
        doNothing().when(batcher2).add(opCaptor2.capture());
        doNothing().when(batcher3).add(opCaptor3.capture());
        var result = client.deleteRange(min, max);
        assertThat(result).isNotCompleted();

        assertThat(opCaptor1.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.minKeyInclusive()).isEqualTo(min);
                            assertThat(o.maxKeyInclusive()).isEqualTo(max);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor2.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.minKeyInclusive()).isEqualTo(min);
                            assertThat(o.maxKeyInclusive()).isEqualTo(max);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor3.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.minKeyInclusive()).isEqualTo(min);
                            assertThat(o.maxKeyInclusive()).isEqualTo(max);
                            assertThat(o.callback()).isNotCompleted();
                        });

        opCaptor1.getValue().callback().complete(null);
        opCaptor2.getValue().callback().complete(null);
        opCaptor3.getValue().callback().complete(null);
        assertThat(result).isCompleted();
    }

    @Test
    void get() {
        var opCaptor = ArgumentCaptor.forClass(GetOperation.class);
        var shardId = 1L;
        var key = "key";
        when(shardManager.get(key)).thenReturn(shardId);
        when(readBatchManager.getBatcher(shardId)).thenReturn(batcher);
        doNothing().when(batcher).add(opCaptor.capture());
        var result = client.get(key);
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.callback()).isSameAs(result);
                        });
    }

    @Test
    void list() {
        var batcher1 = mock(Batcher.class);
        var batcher2 = mock(Batcher.class);
        var batcher3 = mock(Batcher.class);
        var opCaptor1 = ArgumentCaptor.forClass(ListOperation.class);
        var opCaptor2 = ArgumentCaptor.forClass(ListOperation.class);
        var opCaptor3 = ArgumentCaptor.forClass(ListOperation.class);
        var min = "a-min";
        var max = "z-max";
        when(shardManager.getAll()).thenReturn(List.of(1L, 2L, 3L));
        when(readBatchManager.getBatcher(1L)).thenReturn(batcher1);
        when(readBatchManager.getBatcher(2L)).thenReturn(batcher2);
        when(readBatchManager.getBatcher(3L)).thenReturn(batcher3);
        doNothing().when(batcher1).add(opCaptor1.capture());
        doNothing().when(batcher2).add(opCaptor2.capture());
        doNothing().when(batcher3).add(opCaptor3.capture());
        var result = client.list(min, max);
        assertThat(result).isNotCompleted();

        assertThat(opCaptor1.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.minKeyInclusive()).isEqualTo(min);
                            assertThat(o.maxKeyInclusive()).isEqualTo(max);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor2.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.minKeyInclusive()).isEqualTo(min);
                            assertThat(o.maxKeyInclusive()).isEqualTo(max);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor3.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.minKeyInclusive()).isEqualTo(min);
                            assertThat(o.maxKeyInclusive()).isEqualTo(max);
                            assertThat(o.callback()).isNotCompleted();
                        });

        opCaptor1.getValue().callback().complete(List.of("a"));
        opCaptor2.getValue().callback().complete(List.of("b"));
        opCaptor3.getValue().callback().complete(List.of("c"));
        assertThat(result).isCompletedWithValueMatching(l -> l.containsAll(List.of("a", "b", "c")));
    }

    @Test
    void close() throws Exception {
        client.close();
        var inOrder =
                inOrder(
                        readBatchManager, writeBatchManager, notificationManager, shardManager, channelManager);
        inOrder.verify(readBatchManager).close();
        inOrder.verify(writeBatchManager).close();
        inOrder.verify(notificationManager).close();
        inOrder.verify(shardManager).close();
        inOrder.verify(channelManager).close();
    }
}
