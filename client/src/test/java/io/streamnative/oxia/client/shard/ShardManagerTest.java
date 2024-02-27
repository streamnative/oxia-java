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
package io.streamnative.oxia.client.shard;

import static io.streamnative.oxia.client.OxiaClientBuilder.DefaultNamespace;
import static io.streamnative.oxia.client.shard.HashRangeShardStrategy.Xxh332HashRangeShardStrategy;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.CompositeConsumer;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.ShardAssignmentMetrics;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Added;
import io.streamnative.oxia.proto.NamespaceShardsAssignment;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import io.streamnative.oxia.proto.ShardAssignment;
import io.streamnative.oxia.proto.ShardAssignments;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

@ExtendWith(MockitoExtension.class)
public class ShardManagerTest {

    @Nested
    @DisplayName("Tests of assignments data structure")
    class AssignmentsTests {

        @Test
        void recomputeShardHashBoundaries() {
            var existing =
                    Map.of(
                            1L, new Shard(1L, "leader 1", new HashRange(1, 3)),
                            2L, new Shard(2L, "leader 1", new HashRange(7, 9)),
                            3L, new Shard(3L, "leader 2", new HashRange(4, 6)),
                            4L, new Shard(4L, "leader 2", new HashRange(10, 11)),
                            5L, new Shard(5L, "leader 3", new HashRange(11, 12)),
                            6L, new Shard(6L, "leader 4", new HashRange(13, 13)));
            var updates =
                    List.of(
                            new Shard(1L, "leader 4", new HashRange(1, 3)), // Leader change
                            new Shard(2L, "leader 4", new HashRange(7, 9)), //
                            new Shard(3L, "leader 2", new HashRange(4, 5)), // Split
                            new Shard(7L, "leader 3", new HashRange(6, 6)), //
                            new Shard(4L, "leader 2", new HashRange(10, 12)) // Merge
                            );
            var assignments = ShardManager.recomputeShardHashBoundaries(existing, updates);
            assertThat(assignments)
                    .satisfies(
                            a -> {
                                assertThat(a)
                                        .containsOnly(
                                                entry(1L, new Shard(1L, "leader 4", new HashRange(1, 3))),
                                                entry(2L, new Shard(2L, "leader 4", new HashRange(7, 9))),
                                                entry(3L, new Shard(3L, "leader 2", new HashRange(4, 5))),
                                                entry(7L, new Shard(7L, "leader 3", new HashRange(6, 6))),
                                                entry(4L, new Shard(4L, "leader 2", new HashRange(10, 12))),
                                                entry(6L, new Shard(6L, "leader 4", new HashRange(13, 13))));
                                assertThat(a).isUnmodifiable();
                            });
        }

        @Nested
        @DisplayName("Tests of locking behaviour")
        class LockingTests {

            ShardManager.Assignments assignments;

            @Mock ReadWriteLock lock;
            @Mock Lock rLock;
            @Mock Lock wLock;

            @BeforeEach
            void mocking() {
                when(lock.readLock()).thenReturn(rLock);
                when(lock.writeLock()).thenReturn(wLock);
                assignments = new ShardManager.Assignments(lock, s -> k -> true, DefaultNamespace);
                assignments.update(singletonList(new Shard(1, "leader 1", new HashRange(1, 1))));
            }

            @Test
            void get() {
                assignments.get("key");
                var inorder = inOrder(rLock);
                inorder.verify(rLock).lock();
                inorder.verify(rLock).unlock();
            }

            @Test
            void getFail() {
                assignments = new ShardManager.Assignments(lock, s -> k -> false, DefaultNamespace);
                assertThatThrownBy(() -> assignments.get("key"))
                        .isInstanceOf(NoShardAvailableException.class);
                var inorder = inOrder(rLock);
                inorder.verify(rLock).lock();
                inorder.verify(rLock).unlock();
            }

            @Test
            void getAll() {
                assertThat(assignments.getAll()).containsOnly(1L);
                var inorder = inOrder(rLock);
                inorder.verify(rLock).lock();
                inorder.verify(rLock).unlock();
            }

            @Test
            void leader() {
                assertThat(assignments.leader(1L)).isEqualTo("leader 1");
                var inorder = inOrder(rLock);
                inorder.verify(rLock).lock();
                inorder.verify(rLock).unlock();
            }

            @Test
            void leaderFail() {
                assignments = new ShardManager.Assignments(lock, s -> k -> false, DefaultNamespace);
                assertThatThrownBy(() -> assignments.leader(1L))
                        .isInstanceOf(NoShardAvailableException.class);
                var inorder = inOrder(rLock);
                inorder.verify(rLock).lock();
                inorder.verify(rLock).unlock();
            }

            @Test
            void update() {
                assignments.update(emptyList());
                var inorder = inOrder(wLock);
                inorder.verify(wLock).lock();
                inorder.verify(wLock).unlock();
            }
        }
    }

    @Nested
    @DisplayName("Manager delegation")
    class ManagerTests {
        private final String namespace = "default";

        @Spy
        ShardManager.Assignments assignments =
                new ShardManager.Assignments(Xxh332HashRangeShardStrategy, DefaultNamespace);

        @Mock ReactorOxiaClientGrpc.ReactorOxiaClientStub reactor;
        @Mock OxiaStub stub;
        @Mock ShardAssignmentMetrics metrics;
        ShardManager manager;

        @BeforeEach
        void mocking() {
            stub = mock(OxiaStub.class);
            reactor = mock(ReactorOxiaClientGrpc.ReactorOxiaClientStub.class);

            manager = new ShardManager(stub, assignments, new CompositeConsumer<>(), metrics);
        }

        @Test
        void start() {
            var assignment = ShardAssignment.newBuilder().setShardId(0).setLeader("leader0").build();
            var nsAssignment = NamespaceShardsAssignment.newBuilder().addAssignments(assignment).build();

            when(stub.reactor()).thenReturn(reactor);
            when(reactor.getShardAssignments(
                            ShardAssignmentsRequest.newBuilder().setNamespace(namespace).build()))
                    .thenReturn(
                            Flux.just(
                                    ShardAssignments.newBuilder().putNamespaces(namespace, nsAssignment).build()));
            var future = manager.start();
            assertThat(future).succeedsWithin(Duration.ofMillis(100));

            assertThat(manager.leader(0)).isEqualTo("leader0");

            verify(metrics, atLeast(1)).recordAssignments(any());
            verify(metrics, atLeast(1))
                    .recordChanges(
                            new ShardManager.ShardAssignmentChanges(
                                    Set.of(new Added(0, "leader0")), Set.of(), Set.of()));
        }

        @Test
        void get() {
            assertThatThrownBy(() -> manager.get("a")).isInstanceOf(NoShardAvailableException.class);
        }

        @Test
        void getAll() {
            manager.getAll();
            verify(assignments).getAll();
        }

        @Test
        void leader() {
            assertThatThrownBy(() -> manager.leader(1)).isInstanceOf(NoShardAvailableException.class);
        }
    }
}
