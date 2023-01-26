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

import static io.streamnative.oxia.client.shard.ModelFactory.newShardAssignments;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientImplBase;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientStub;
import io.streamnative.oxia.proto.ShardAssignments;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ShardManagerGrpcTest {

    private BlockingQueue<StreamResponse> responses = new ArrayBlockingQueue<>(10);
    private final ScheduledExecutorService responseSender =
            Executors.newSingleThreadScheduledExecutor();
    private final CompletableFuture<Void> test = new CompletableFuture<>();
    private final AtomicLong shardAssignmentsCount = new AtomicLong();

    private final OxiaClientImplBase serviceImpl =
            mock(
                    OxiaClientImplBase.class,
                    delegatesTo(
                            new OxiaClientImplBase() {
                                @Override
                                @SneakyThrows
                                public void shardAssignments(
                                        ShardAssignmentsRequest request, StreamObserver<ShardAssignments> observer) {
                                    shardAssignmentsCount.incrementAndGet();
                                    responseSender.execute(
                                            () -> {
                                                var streamDone = false;
                                                var queue = responses;
                                                while (!test.isDone() && !streamDone) {
                                                    try {
                                                        var r = queue.take();
                                                        if (r instanceof StreamResponse.Error e) {
                                                            streamDone = true;
                                                            queue = null;
                                                            observer.onError(e.throwable());
                                                        } else if (r instanceof StreamResponse.Completed c) {
                                                            streamDone = true;
                                                            queue = null;
                                                            observer.onCompleted();
                                                        } else if (r instanceof StreamResponse.Assignments a) {
                                                            observer.onNext(a.response);
                                                        }
                                                    } catch (InterruptedException e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            });
                                }
                            }));

    private Function<String, OxiaClientStub> clientByShardId;
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    public void setUp() throws Exception {
        responses.clear();
        String serverName = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        clientByShardId = s -> OxiaClientGrpc.newStub(channel);
    }

    @AfterEach
    void tearDown() {
        test.complete(null);
        server.shutdownNow();
        channel.shutdownNow();
        responseSender.shutdownNow();
    }

    @Test
    public void start() throws Exception {
        try (var shardManager = new ShardManager("address", clientByShardId)) {
            var bootstrap = shardManager.start();
            assertThat(bootstrap).isNotCompleted();
            assertThat(shardManager.getAll()).isEmpty();
            responses.add(assignments(newShardAssignments(1, 2, 3, "leader 1")));
            await("bootstrapping").until(bootstrap::isDone);
            assertThat(bootstrap).isNotCompletedExceptionally();
            assertThat(shardManager.getAll()).containsOnly(1);
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
        }
    }

    @Test
    public void update() throws Exception {
        var s1v1 = newShardAssignments(1, 2, 5, "leader 1");
        var s1v2 = newShardAssignments(1, 2, 3, "leader 2");
        var strategy = new StaticShardStrategy().assign("key1", s1v1).assign("key2", s1v1);
        try (var shardManager = new ShardManager(strategy, "address", clientByShardId)) {
            responses.add(assignments(s1v1));
            shardManager.start().get();
            assertThat(shardManager.get("key1")).isEqualTo(1);
            assertThat(shardManager.get("key2")).isEqualTo(1);
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
            strategy.assign("key1", s1v2);
            responses.add(assignments(s1v2));
            await("application of update")
                    .untilAsserted(
                            () -> {
                                assertThat(shardManager.get("key1")).isEqualTo(1);
                                assertThatThrownBy(() -> shardManager.get("key2"))
                                        .isInstanceOf(IllegalStateException.class);
                                assertThat(shardManager.leader(1)).isEqualTo("leader 2");
                            });
            // s1v1 mapped to both k1,k2 -- but s1v2 will only map to k1 -- therefore s1 must have been
            // updated to s1v2
        }
    }

    @Test
    public void overlap() throws Exception {
        var s1 = newShardAssignments(1, 1, 3, "leader 1");
        var s2 = newShardAssignments(2, 2, 4, "leader 2");
        var strategy = new StaticShardStrategy().assign("key1", s1);
        try (var shardManager = new ShardManager(strategy, "address", clientByShardId)) {
            responses.add(assignments(s1));
            shardManager.start().get();
            assertThat(shardManager.get("key1")).isEqualTo(1);
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
            strategy.assign("key2", s2);
            responses.add(assignments(s2));
            await("removal of shard 1")
                    .untilAsserted(
                            () -> {
                                assertThatThrownBy(() -> shardManager.get("key1"))
                                        .isInstanceOf(IllegalStateException.class);
                                assertThatThrownBy(() -> shardManager.leader(1))
                                        .isInstanceOf(IllegalStateException.class);
                                assertThat(shardManager.get("key2")).isEqualTo(2);
                                assertThat(shardManager.leader(2)).isEqualTo("leader 2");
                            }
                            // s1 no longer exists -- it was removed by overlapping s2 -- and thus k1 can no
                            // longer map to it
                            );
        }
    }

    @Test
    public void recoveryFromError() throws Exception {
        try (var shardManager = new ShardManager("address", clientByShardId)) {
            responses.add(assignments(1, 2, 3, "leader 1"));
            shardManager.start().get();
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
            responses.add(error());
            await("next request").untilAsserted(() -> assertThat(shardAssignmentsCount).hasValue(2));
            responses.add(assignments(1, 2, 3, "leader 2"));
            await("recovering to leader 2")
                    .untilAsserted(() -> assertThat(shardManager.leader(1)).isEqualTo("leader 2"));
        }
    }

    @Test
    public void recoveryFromEndOfStream() throws Exception {
        try (var shardManager = new ShardManager("address", clientByShardId)) {
            var bootstrapped = shardManager.start();
            await("next request").untilAsserted(() -> assertThat(shardAssignmentsCount).hasValue(1));
            responses.add(assignments(1, 2, 3, "leader 1"));
            bootstrapped.get();
            assertThat(shardManager.leader(1)).isEqualTo("leader 1");
            responses.add(completed());
            await("next request").untilAsserted(() -> assertThat(shardAssignmentsCount).hasValue(2));
            responses.add(assignments(1, 2, 3, "leader 2"));
            await("recovering to leader 2")
                    .untilAsserted(() -> assertThat(shardManager.leader(1)).isEqualTo("leader 2"));
        }
    }

    sealed interface StreamResponse
            permits StreamResponse.Completed, StreamResponse.Assignments, StreamResponse.Error {
        record Error(Throwable throwable) implements StreamResponse {}

        enum Completed implements StreamResponse {
            INSTANCE;
        }

        record Assignments(ShardAssignments response) implements StreamResponse {}
    }

    static StreamResponse.Assignments assignments(int id, int min, int max, String leader) {
        return new StreamResponse.Assignments(newShardAssignments(id, min, max, leader));
    }

    static StreamResponse.Assignments assignments(ShardAssignments r) {
        return new StreamResponse.Assignments(r);
    }

    static StreamResponse.Completed completed() {
        return StreamResponse.Completed.INSTANCE;
    }

    static StreamResponse.Error error() {
        return new StreamResponse.Error(new RuntimeException());
    }
}
