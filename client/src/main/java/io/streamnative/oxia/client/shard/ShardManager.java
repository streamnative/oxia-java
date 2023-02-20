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

import static io.streamnative.oxia.client.shard.HashRangeShardStrategy.Xxh332HashRangeShardStrategy;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.CompositeConsumer;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import io.streamnative.oxia.proto.ShardAssignments;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
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
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

@Slf4j
public class ShardManager extends GrpcResponseStream implements AutoCloseable {
    private final @NonNull Assignments assignments;
    private final @NonNull CompositeConsumer<Assignments> callbacks;

    @VisibleForTesting
    ShardManager(
            @NonNull Supplier<ReactorOxiaClientStub> stubFactory, @NonNull Assignments assignments,
            @NonNull CompositeConsumer<Assignments> callbacks) {
        super(stubFactory);
        this.assignments = assignments;
        this.callbacks = callbacks;
    }

    public ShardManager(@NonNull Supplier<ReactorOxiaClientStub> stubFactory) {
        this(stubFactory, new Assignments(Xxh332HashRangeShardStrategy), new CompositeConsumer<>());
    }

    @Override
    protected CompletableFuture<Void> start(
            ReactorOxiaClientStub stub, Consumer<Disposable> consumer) {
        // TODO filter non-retriables?
        RetryBackoffSpec retrySpec =
                Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                        .doBeforeRetry(signal -> log.warn("Retrying receiving shard assignments: {}", signal));
        var assignmentsFlux =
                stub.getShardAssignments(ShardAssignmentsRequest.getDefaultInstance())
                        .doOnError(t -> log.warn("Error receiving shard assignments", t))
                        .retryWhen(retrySpec)
                        .repeat()
                        .doOnNext(this::updateAssignments)
                        .publish();
        // Complete after the first response has been processed
        var future = Mono.from(assignmentsFlux).then().toFuture();
        var disposable = assignmentsFlux.connect();
        consumer.accept(disposable);
        return future;
    }

    private void updateAssignments(ShardAssignments shardAssignments) {
        var updates =
                shardAssignments.getAssignmentsList().stream().map(Shard::fromProto).collect(toList());
        assignments.update(updates);
        callbacks.accept(assignments);
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

    public void addCallback(@NonNull Consumer<Assignments> callback) {
        callbacks.add(callback);
    }

    public static class Assignments {
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
}
