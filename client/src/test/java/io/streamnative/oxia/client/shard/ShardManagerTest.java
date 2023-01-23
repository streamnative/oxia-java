package io.streamnative.oxia.client.shard;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.streamnative.oxia.client.grpc.Receiver;
import io.streamnative.oxia.proto.ShardAssignmentsResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ShardManagerTest {

    @Nested
    @DisplayName("Tests of assignments data structure")
    class AssignmentsTests {

        @Test
        void applyUpdates() {
            var existing =
                    Map.of(
                            1, new Shard(1, "leader 1", new HashRange(1, 3)),
                            2, new Shard(2, "leader 1", new HashRange(7, 9)),
                            3, new Shard(3, "leader 2", new HashRange(4, 6)),
                            4, new Shard(4, "leader 2", new HashRange(10, 11)),
                            5, new Shard(5, "leader 3", new HashRange(11, 12)),
                            6, new Shard(6, "leader 4", new HashRange(13, 13)));
            var updates =
                    List.of(
                            new Shard(1, "leader 4", new HashRange(1, 3)), // Leader change
                            new Shard(2, "leader 4", new HashRange(7, 9)), //
                            new Shard(3, "leader 2", new HashRange(4, 5)), // Split
                            new Shard(7, "leader 3", new HashRange(6, 6)), //
                            new Shard(4, "leader 2", new HashRange(10, 12)) // Merge
                            );
            var assignments = ShardManager.Assignments.applyUpdates(existing, updates);
            assertThat(assignments)
                    .satisfies(
                            a -> {
                                assertThat(a)
                                        .containsOnly(
                                                entry(1, new Shard(1, "leader 4", new HashRange(1, 3))),
                                                entry(2, new Shard(2, "leader 4", new HashRange(7, 9))),
                                                entry(3, new Shard(3, "leader 2", new HashRange(4, 5))),
                                                entry(7, new Shard(7, "leader 3", new HashRange(6, 6))),
                                                entry(4, new Shard(4, "leader 2", new HashRange(10, 12))),
                                                entry(6, new Shard(6, "leader 4", new HashRange(13, 13))));
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
                assignments = new ShardManager.Assignments(lock, s -> k -> true);
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
                assignments = new ShardManager.Assignments(lock, s -> k -> false);
                assertThatThrownBy(() -> assignments.get("key")).isInstanceOf(IllegalStateException.class);
                var inorder = inOrder(rLock);
                inorder.verify(rLock).lock();
                inorder.verify(rLock).unlock();
            }

            @Test
            void getAll() {
                assertThat(assignments.getAll()).containsOnly(1);
                var inorder = inOrder(rLock);
                inorder.verify(rLock).lock();
                inorder.verify(rLock).unlock();
            }

            @Test
            void leader() {
                assertThat(assignments.leader(1)).isEqualTo("leader 1");
                var inorder = inOrder(rLock);
                inorder.verify(rLock).lock();
                inorder.verify(rLock).unlock();
            }

            @Test
            void leaderFail() {
                assignments = new ShardManager.Assignments(lock, s -> k -> false);
                assertThatThrownBy(() -> assignments.leader(1)).isInstanceOf(IllegalStateException.class);
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
    @DisplayName("Tests for the GRPC Stream Observer")
    class ObserverTests {
        @Test
        void observerOnNext() {
            @SuppressWarnings("unchecked")
            Consumer<ShardAssignmentsResponse> consumer =
                    (Consumer<ShardAssignmentsResponse>) mock(Consumer.class);
            var terminal = new CompletableFuture<Void>();
            var observer = new ShardManager.ShardAssignmentsObserver(terminal, consumer);
            var response = ShardAssignmentsResponse.getDefaultInstance();
            observer.onNext(response);
            verify(consumer).accept(response);
            assertThat(terminal).isNotCompleted();
        }

        @Test
        void observerComplete() {
            var terminal = new CompletableFuture<Void>();
            var observer = new ShardManager.ShardAssignmentsObserver(terminal, r -> {});
            observer.onCompleted();
            assertThat(terminal).isCompleted();
            assertThat(terminal).isNotCompletedExceptionally();
        }

        @Test
        void observerError() {
            var terminal = new CompletableFuture<Void>();
            var observer = new ShardManager.ShardAssignmentsObserver(terminal, r -> {});
            observer.onError(new RuntimeException());
            assertThat(terminal).isCompletedExceptionally();
        }
    }

    @Nested
    @DisplayName("Manager delegation")
    class ManagerTests {
        @Mock ShardManager.Assignments assignments;
        @Mock CompletableFuture<Void> bootstrap;
        @Mock Receiver receiver;
        ShardManager manager;

        @BeforeEach
        void mocking() {
            manager = new ShardManager(receiver, assignments);
        }

        @Test
        void start() {
            when(receiver.bootstrap()).thenReturn(bootstrap);
            var future = manager.start();
            assertThat(future).isEqualTo(bootstrap);
            verify(receiver).receive();
        }

        @Test
        void close() throws Exception {
            manager.close();
            verify(receiver).close();
        }

        @Test
        void get() {
            manager.get("a");
            verify(assignments).get("a");
        }

        @Test
        void getAll() {
            manager.getAll();
            verify(assignments).getAll();
        }

        @Test
        void leader() {
            manager.leader(1);
            verify(assignments).leader(1);
        }
    }
}
