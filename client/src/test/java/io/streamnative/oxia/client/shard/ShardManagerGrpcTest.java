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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doReturn;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.streamnative.oxia.proto.Int32HashRange;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.OxiaClientImplBase;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import io.streamnative.oxia.proto.ShardAssignment;
import io.streamnative.oxia.proto.ShardAssignments;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ShardManagerGrpcTest {
    BlockingQueue<Flux<ShardAssignments>> responses = new ArrayBlockingQueue<>(10);

    OxiaClientImplBase serviceImpl =
            new OxiaClientImplBase() {
                @Override
                public Flux<ShardAssignments> getShardAssignments(Mono<ShardAssignmentsRequest> request) {
                    requests.incrementAndGet();
                    Flux<ShardAssignments> assignments = responses.poll();
                    if (assignments == null) {
                        return Flux.error(Status.RESOURCE_EXHAUSTED.asException());
                    }
                    return assignments;
                }
            };
    AtomicInteger requests = new AtomicInteger();

    String serverName = InProcessServerBuilder.generateName();
    Server server;
    ManagedChannel channel;
    @Mock Supplier<ReactorOxiaClientStub> stubFactory;

    @BeforeEach
    void beforeEach() throws Exception {
        requests.set(0);
        responses.clear();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        var stub = ReactorOxiaClientGrpc.newReactorStub(channel);
        doReturn(stub).when(stubFactory).get();
    }

    @AfterEach
    void afterEach() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void start() {
        var assignments = ShardAssignments.newBuilder().addAssignments(assignment(0, 0, 3)).build();
        responses.offer(Flux.just(assignments).concatWith(Flux.never()));
        try (var shardManager = new ShardManager(stubFactory)) {
            assertThat(shardManager.start()).succeedsWithin(Duration.ofSeconds(1));
            assertThat(shardManager.getAll()).containsExactlyInAnyOrder(0L);
            assertThat(shardManager.leader(0)).isEqualTo("leader0");
        }
    }

    @Test
    void neverStarts() {
        responses.offer(Flux.never());
        try (var shardManager = new ShardManager(stubFactory)) {
            assertThatThrownBy(() -> shardManager.start().get(1, SECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(shardManager.getAll()).isEmpty();
        }
    }

    @Test
    void update() {
        var assignments0 = ShardAssignments.newBuilder().addAssignments(assignment(0, 0, 3)).build();
        var assignments1 =
                ShardAssignments.newBuilder()
                        .addAssignments(assignment(1, 0, 1))
                        .addAssignments(assignment(2, 2, 3))
                        .build();
        responses.offer(Flux.just(assignments0, assignments1).concatWith(Flux.never()));
        try (var shardManager = new ShardManager(stubFactory)) {
            shardManager.start().join();
            await()
                    .untilAsserted(
                            () -> {
                                assertThat(shardManager.getAll()).containsExactlyInAnyOrder(1L, 2L);
                                assertThat(shardManager.leader(1)).isEqualTo("leader1");
                                assertThat(shardManager.leader(2)).isEqualTo("leader2");
                            });
        }
    }

    @Test
    public void recoveryFromError() {
        responses.offer(Flux.error(Status.UNAVAILABLE.asException()));
        var assignments = ShardAssignments.newBuilder().addAssignments(assignment(0, 0, 3)).build();
        responses.offer(Flux.just(assignments).concatWith(Flux.never()));
        try (var shardManager = new ShardManager(stubFactory)) {
            assertThat(shardManager.start()).succeedsWithin(Duration.ofSeconds(1));
            assertThat(shardManager.getAll()).containsExactlyInAnyOrder(0L);
            assertThat(shardManager.leader(0)).isEqualTo("leader0");
        }
        assertThat(requests).hasValue(2);
    }

    @Test
    public void recoveryFromEndOfStream() {
        responses.offer(Flux.empty());
        var assignments = ShardAssignments.newBuilder().addAssignments(assignment(0, 0, 3)).build();
        responses.offer(Flux.just(assignments).concatWith(Flux.never()));
        try (var shardManager = new ShardManager(stubFactory)) {
            assertThat(shardManager.start()).succeedsWithin(Duration.ofSeconds(1));
            assertThat(shardManager.getAll()).containsExactlyInAnyOrder(0L);
            assertThat(shardManager.leader(0)).isEqualTo("leader0");
        }
        assertThat(requests).hasValue(2);
    }

    ShardAssignment assignment(int shardId, int min, int max) {
        var hashRange =
                Int32HashRange.newBuilder().setMinHashInclusive(min).setMaxHashInclusive(max).build();
        return ShardAssignment.newBuilder()
                .setShardId(shardId)
                .setLeader("leader" + shardId)
                .setInt32HashRange(hashRange)
                .build();
    }
}
