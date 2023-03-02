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

import static io.streamnative.oxia.client.api.PutOption.ifVersionIdEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.batch.BatchManager;
import io.streamnative.oxia.client.batch.Batcher;
import io.streamnative.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.streamnative.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.streamnative.oxia.client.grpc.ChannelManager;
import io.streamnative.oxia.client.grpc.ChannelManager.StubFactory;
import io.streamnative.oxia.client.notify.NotificationManager;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.ListRequest;
import io.streamnative.oxia.proto.ListResponse;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

@ExtendWith(MockitoExtension.class)
class AsyncOxiaClientImplTest {

    @Mock ChannelManager channelManager;
    @Mock ShardManager shardManager;
    @Mock NotificationManager notificationManager;
    @Mock BatchManager readBatchManager;
    @Mock BatchManager writeBatchManager;
    @Mock SessionManager sessionManager;
    @Mock Batcher batcher;
    @Mock StubFactory<ReactorOxiaClientStub> reactorStubFactory;

    AsyncOxiaClientImpl client;

    @BeforeEach
    void setUp() {
        client =
                new AsyncOxiaClientImpl(
                        channelManager,
                        shardManager,
                        notificationManager,
                        readBatchManager,
                        writeBatchManager,
                        sessionManager,
                        reactorStubFactory);
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
                            assertThat(o.expectedVersionId()).isEmpty();
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
        var result = client.put(key, value, ifVersionIdEquals(expectedVersionId));
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).hasValue(expectedVersionId);
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
                            assertThat(o.expectedVersionId()).isEmpty();
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
        var result = client.delete(key, DeleteOption.ifVersionIdEquals(expectedVersionId));
        assertThat(result).isNotCompleted();
        assertThat(opCaptor.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.key()).isEqualTo(key);
                            assertThat(o.expectedVersionId()).hasValue(expectedVersionId);
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
        var startInclusive = "a-startInclusive";
        var endExclusive = "z-endExclusive";
        when(shardManager.getAll()).thenReturn(List.of(1L, 2L, 3L));
        when(writeBatchManager.getBatcher(1L)).thenReturn(batcher1);
        when(writeBatchManager.getBatcher(2L)).thenReturn(batcher2);
        when(writeBatchManager.getBatcher(3L)).thenReturn(batcher3);
        doNothing().when(batcher1).add(opCaptor1.capture());
        doNothing().when(batcher2).add(opCaptor2.capture());
        doNothing().when(batcher3).add(opCaptor3.capture());
        var result = client.deleteRange(startInclusive, endExclusive);
        assertThat(result).isNotCompleted();

        assertThat(opCaptor1.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor2.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
                            assertThat(o.callback()).isNotCompleted();
                        });

        assertThat(opCaptor3.getValue())
                .satisfies(
                        o -> {
                            assertThat(o.startKeyInclusive()).isEqualTo(startInclusive);
                            assertThat(o.endKeyExclusive()).isEqualTo(endExclusive);
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
    void list(@Mock ReactorOxiaClientStub stub0, @Mock ReactorOxiaClientStub stub1) {
        when(shardManager.getAll()).thenReturn(List.of(0L, 1L));
        setupListStub(0L, "leader0", stub0);
        setupListStub(1L, "leader1", stub1);

        List<String> list = client.list("a", "e").join();

        assertThat(list)
                .containsExactlyInAnyOrder("0-a", "0-b", "0-c", "0-d", "1-a", "1-b", "1-c", "1-d");
    }

    private void setupListStub(long shardId, String leader, ReactorOxiaClientStub stub) {
        when(shardManager.leader(shardId)).thenReturn(leader);
        when(reactorStubFactory.apply(leader)).thenReturn(stub);
        when(stub.list(any(ListRequest.class)))
                .thenReturn(
                        Flux.just(listResponse(shardId, "a", "b"), listResponse(shardId, "c", "d"))
                                .delayElements(Duration.ofMillis(1)));
    }

    private ListResponse listResponse(long shardId, String first, String second) {
        return ListResponse.newBuilder()
                .addAllKeys(List.of(shardId + "-" + first, shardId + "-" + second))
                .build();
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
