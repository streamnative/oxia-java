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

import static io.streamnative.oxia.client.shard.HashRangeShardStrategy.Xxh332HashRangeShardStrategy;
import static io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Added;
import static io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Reassigned;
import static io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Removed;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.streamnative.oxia.client.CompositeConsumer;
import io.streamnative.oxia.client.grpc.CustomStatusCode;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.ShardAssignmentMetrics;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.proto.ShardAssignments;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

@Slf4j
public class ShardManager extends GrpcResponseStream implements AutoCloseable {
    private final @NonNull Assignments assignments;
    private final @NonNull CompositeConsumer<ShardAssignmentChanges> callbacks;
    private final @NonNull ShardAssignmentMetrics metrics;

    private final Scheduler scheduler;

    @VisibleForTesting
    ShardManager(
            @NonNull OxiaStub stub,
            @NonNull Assignments assignments,
            @NonNull CompositeConsumer<ShardAssignmentChanges> callbacks,
            @NonNull ShardAssignmentMetrics metrics) {
        super(stub);
        this.assignments = assignments;
        this.callbacks = callbacks;
        this.metrics = metrics;
        this.scheduler = Schedulers.newSingle("shard-assignments");
    }

    public ShardManager(@NonNull OxiaStub stub, @NonNull Metrics metrics, @NonNull String namespace) {
        this(
                stub,
                new Assignments(Xxh332HashRangeShardStrategy, namespace),
                new CompositeConsumer<>(),
                ShardAssignmentMetrics.create(metrics));
    }

    @Override
    public void close() {
        super.close();
        scheduler.dispose();
    }

    @Override
    protected CompletableFuture<Void> start(OxiaStub stub, Consumer<Disposable> consumer) {
        RetryBackoffSpec retrySpec =
                Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                        .filter(this::isErrorRetryable)
                        .doBeforeRetry(signal -> log.warn("Retrying receiving shard assignments: {}", signal));
        var assignmentsFlux =
                Flux.defer(
                                () ->
                                        stub.reactor()
                                                .getShardAssignments(
                                                        ShardAssignmentsRequest.newBuilder()
                                                                .setNamespace(assignments.namespace)
                                                                .build()))
                        .doOnError(this::processError)
                        .retryWhen(retrySpec)
                        .repeat()
                        .publishOn(scheduler)
                        .doOnNext(this::updateAssignments)
                        .doOnEach(metrics::recordAssignments)
                        .publish();
        // Complete after the first response has been processed
        var future = Mono.from(assignmentsFlux).then().toFuture();
        var disposable = assignmentsFlux.connect();
        consumer.accept(disposable);
        return future;
    }

    /**
     * Process errors of repeated flux, it will auto suppress unknown status #{@link
     * StatusRuntimeException}.
     *
     * @param error Error from flux
     */
    private void processError(@NonNull Throwable error) {
        if (error instanceof StatusRuntimeException statusError) {
            var status = statusError.getStatus();
            if (status.getCode() == Status.Code.UNKNOWN) {
                // Suppress unknown errors
                final var description = status.getDescription();
                if (description != null) {
                    var customStatusCode = CustomStatusCode.fromDescription(description);
                    if (customStatusCode == CustomStatusCode.ErrorNamespaceNotFound) {
                        final var ex = new NamespaceNotFoundException(assignments.namespace);
                        log.error("Failed receiving shard assignments", ex);
                        throw ex;
                    }
                }
            }
        }
        log.warn("Failed receiving shard assignments", error);
    }

    private void updateAssignments(ShardAssignments shardAssignments) {
        var nsSharedAssignments = shardAssignments.getNamespacesMap().get(assignments.namespace);
        if (nsSharedAssignments == null) {
            /*
            It shouldn't happen, but we have to do some defensive programming to avoid server nodes
            Allowing namespaces to be deleted in the future.

            Retries are available so that the client does not panic until the namespace is recreated.
            */
            throw new NamespaceNotFoundException(assignments.namespace, true);
        }
        var updates =
                nsSharedAssignments.getAssignmentsList().stream().map(Shard::fromProto).collect(toList());
        var updatedMap = recomputeShardHashBoundaries(assignments.shards, updates);
        var changes = computeShardLeaderChanges(assignments.shards, updatedMap);
        assignments.update(updates);
        callbacks.accept(changes);
        metrics.recordChanges(changes);
    }

    @VisibleForTesting
    static Map<Long, Shard> recomputeShardHashBoundaries(
            Map<Long, Shard> assignments, List<Shard> updates) {
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

    @VisibleForTesting
    static ShardAssignmentChanges computeShardLeaderChanges(
            Map<Long, Shard> oldAssignments, Map<Long, Shard> newAssignments) {
        Set<Removed> removed =
                oldAssignments.entrySet().stream()
                        .filter(e -> !newAssignments.containsKey(e.getKey()))
                        .map(e -> new Removed(e.getKey(), e.getValue().leader()))
                        .collect(toSet());
        Set<Added> added =
                newAssignments.entrySet().stream()
                        .filter(e -> !oldAssignments.containsKey(e.getKey()))
                        .map(e -> new Added(e.getKey(), e.getValue().leader()))
                        .collect(toSet());
        Set<Reassigned> changed =
                oldAssignments.entrySet().stream()
                        .filter(e -> newAssignments.containsKey(e.getKey()))
                        .filter(e -> !newAssignments.get(e.getKey()).leader().equals(e.getValue().leader()))
                        .map(
                                e -> {
                                    var shardId = e.getKey();
                                    var oldLeader = e.getValue().leader();
                                    var newLeader = newAssignments.get(e.getKey()).leader();
                                    return new Reassigned(shardId, oldLeader, newLeader);
                                })
                        .collect(toSet());
        return new ShardAssignmentChanges(
                unmodifiableSet(added), unmodifiableSet(removed), unmodifiableSet(changed));
    }

    public record ShardAssignmentChanges(
            Set<Added> added, Set<Removed> removed, Set<Reassigned> reassigned) {}

    public sealed interface ShardAssignmentChange permits Added, Removed, Reassigned {
        record Added(long shardId, @NonNull String leader) implements ShardAssignmentChange {}

        record Removed(long shardId, @NonNull String leader) implements ShardAssignmentChange {}

        record Reassigned(long shardId, @NonNull String fromLeader, @NonNull String toLeader)
                implements ShardAssignmentChange {}
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

    public void addCallback(@NonNull Consumer<ShardAssignmentChanges> callback) {
        callbacks.add(callback);
    }

    public static class Assignments {
        private final Lock rLock;
        private final Lock wLock;
        private Map<Long, Shard> shards = new HashMap<>();
        private final ShardStrategy shardStrategy;
        private final String namespace;

        Assignments(ShardStrategy shardStrategy, String namespace) {
            this(new ReentrantReadWriteLock(), shardStrategy, namespace);
        }

        Assignments(ReadWriteLock lock, ShardStrategy shardStrategy, String namespace) {
            if (Strings.isNullOrEmpty(namespace)) {
                throw new IllegalArgumentException("namespace must not be null or empty");
            }
            this.shardStrategy = shardStrategy;
            this.namespace = namespace;
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
                shards = recomputeShardHashBoundaries(shards, updates);
            } finally {
                wLock.unlock();
            }
        }
    }

    private boolean isErrorRetryable(@NonNull Throwable ex) {
        if (ex instanceof NamespaceNotFoundException nsNotFoundError) {
            return nsNotFoundError.isRetryable();
        }
        // Allow the rest of the errors to retry.
        return true;
    }
}
