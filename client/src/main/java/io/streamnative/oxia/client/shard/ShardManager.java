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

import static io.grpc.Status.fromThrowable;
import static io.streamnative.oxia.client.shard.HashRangeShardStrategy.Xxh332HashRangeShardStrategy;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PACKAGE;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.client.grpc.ReceiveWithRecovery;
import io.streamnative.oxia.client.grpc.Receiver;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientStub;
import io.streamnative.oxia.proto.ShardAssignments;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor(access = PACKAGE)
@Slf4j
public class ShardManager implements AutoCloseable {
    private final @NonNull Assignments assignments;
    private final @NonNull Receiver receiver;
    private final @NonNull String serviceAddress;

    public ShardManager(
            @NonNull Function<String, OxiaClientStub> stubFactory, @NonNull String serviceAddress) {
        this(Xxh332HashRangeShardStrategy, stubFactory, serviceAddress);
    }

    @VisibleForTesting
    ShardManager(
            @NonNull ShardStrategy strategy,
            @NonNull Function<String, OxiaClientStub> stubFactory,
            @NonNull String serviceAddress) {
        assignments = new Assignments(strategy);
        receiver = new ReceiveWithRecovery(new GrpcReceiver(serviceAddress, stubFactory, assignments));
        this.serviceAddress = serviceAddress;
    }

    public CompletableFuture<Void> start() {
        receiver.receive();
        return receiver.bootstrap();
    }

    public long get(String key) {
        return assignments.get(key);
    }

    public List<Long> getAll() {
        return assignments.getAll();
    }

    public String leader(long shardId) {
        return assignments.leader(shardId);
    }

    @Override
    public void close() throws Exception {
        receiver.close();
    }

    @VisibleForTesting
    static class Assignments {
        private final Lock rLock;
        private final Lock wLock;
        private Map<Long, Shard> shards = new HashMap<>();
        private final ShardStrategy shardStrategy;

        Assignments(ShardStrategy shardStrategy) {
            this(new ReentrantReadWriteLock(), shardStrategy);
        }

        Assignments(ReadWriteLock lock, ShardStrategy shardStrategy) {
            this.shardStrategy = shardStrategy;
            rLock = lock.readLock();
            wLock = lock.writeLock();
        }

        public long get(String key) {
            try {
                rLock.lock();
                var test = shardStrategy.acceptsKeyPredicate(key);
                var shard = shards.values().stream().filter(test).findAny();
                return shard.map(Shard::id).orElseThrow(() -> new NoShardAvailableException(key));
            } finally {
                rLock.unlock();
            }
        }

        public List<Long> getAll() {
            try {
                rLock.lock();
                return shards.keySet().stream().toList();
            } finally {
                rLock.unlock();
            }
        }

        public String leader(long shardId) {
            try {
                rLock.lock();
                return Optional.ofNullable(shards.get(shardId))
                        .map(Shard::leader)
                        .orElseThrow(() -> new NoShardAvailableException(shardId));
            } finally {
                rLock.unlock();
            }
        }

        void update(List<Shard> updates) {
            try {
                wLock.lock();
                shards = applyUpdates(shards, updates);
            } finally {
                wLock.unlock();
            }
        }

        @VisibleForTesting
        static Map<Long, Shard> applyUpdates(Map<Long, Shard> assignments, List<Shard> updates) {
            var toDelete = new ArrayList<>();
            updates.forEach(
                    update ->
                            update
                                    .findOverlapping(assignments.values())
                                    .forEach(
                                            existing -> {
                                                log.info("Deleting shard {} as it overlaps with {}", existing, update);
                                                toDelete.add(existing.id());
                                            }));
            return unmodifiableMap(
                    Stream.concat(
                                    assignments.entrySet().stream()
                                            .filter(e -> !toDelete.contains(e.getKey()))
                                            .map(Map.Entry::getValue),
                                    updates.stream())
                            .collect(toMap(Shard::id, identity())));
        }
    }

    @RequiredArgsConstructor(access = PACKAGE)
    @VisibleForTesting
    static class GrpcReceiver implements Receiver {
        private final @NonNull String serviceAddress;
        private final @NonNull Function<String, OxiaClientStub> stubFactory;
        private final @NonNull Assignments assignments;
        private final @NonNull CompletableFuture<Void> bootstrap;
        private final @NonNull Supplier<CompletableFuture<Void>> streamTerminalSupplier;

        GrpcReceiver(
                @NonNull String serviceAddress,
                @NonNull Function<String, OxiaClientStub> stubFactory,
                @NonNull Assignments assignments) {
            this(
                    serviceAddress,
                    stubFactory,
                    assignments,
                    new CompletableFuture<>(),
                    CompletableFuture::new);
        }

        public @NonNull CompletableFuture<Void> receive() {
            var terminal = streamTerminalSupplier.get();
            try {
                var observer =
                        new ShardAssignmentsObserver(
                                terminal,
                                s -> {
                                    var updates =
                                            s.getAssignmentsList().stream().map(Shard::fromProto).collect(toList());
                                    assignments.update(updates);
                                    // Signal to the manager that we have some initial shard assignments
                                    bootstrap.complete(null);
                                });
                // Start the stream
                var client = stubFactory.apply(serviceAddress);
                client.getShardAssignments(ShardAssignmentsRequest.getDefaultInstance(), observer);
            } catch (Exception e) {
                terminal.completeExceptionally(e);
            }
            return terminal;
        }

        @Override
        public @NonNull CompletableFuture<Void> bootstrap() {
            return bootstrap;
        }

        @Override
        public void close() {}
    }

    @RequiredArgsConstructor(access = PACKAGE)
    @VisibleForTesting
    static class ShardAssignmentsObserver implements StreamObserver<ShardAssignments> {
        private final CompletableFuture<Void> streamTerminal;
        private final Consumer<ShardAssignments> shardAssignmentsConsumer;

        @Override
        public void onNext(ShardAssignments shardAssignments) {
            shardAssignmentsConsumer.accept(shardAssignments);
        }

        @SneakyThrows
        @Override
        public void onError(Throwable t) {
            log.error("Failed receiving shard assignments - GRPC status: {}", fromThrowable(t));
            // Stream is broken, signal that recovery is necessary
            streamTerminal.completeExceptionally(t);
        }

        @SneakyThrows
        @Override
        public void onCompleted() {
            log.info("Shard Assignment stream completed.");
            // Stream is broken, signal that recovery is necessary
            streamTerminal.complete(null);
        }
    }
}
