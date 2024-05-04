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
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.api.common.Attributes;
import io.streamnative.oxia.client.CompositeConsumer;
import io.streamnative.oxia.client.grpc.CustomStatusCode;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.Counter;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.client.metrics.Unit;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
    private final @NonNull ShardAssignmentsContainer assignments;
    private final @NonNull CompositeConsumer<ShardAssignmentChanges> callbacks;

    private final Counter shardAssignmentsEvents;

    private final Scheduler scheduler;

    @VisibleForTesting
    ShardManager(
            @NonNull OxiaStub stub,
            @NonNull ShardAssignmentsContainer assignments,
            @NonNull CompositeConsumer<ShardAssignmentChanges> callbacks,
            @NonNull InstrumentProvider instrumentProvider) {
        super(stub);
        this.assignments = assignments;
        this.callbacks = callbacks;
        this.scheduler = Schedulers.newSingle("shard-assignments");

        this.shardAssignmentsEvents =
                instrumentProvider.newCounter(
                        "oxia.client.shard.assignments.count",
                        Unit.None,
                        "The total count of received shard assignment events",
                        Attributes.empty());
    }

    public ShardManager(
            @NonNull OxiaStub stub,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull String namespace) {
        this(
                stub,
                new ShardAssignmentsContainer(Xxh332HashRangeShardStrategy, namespace),
                new CompositeConsumer<>(),
                instrumentProvider);
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
                                                                .setNamespace(assignments.getNamespace())
                                                                .build()))
                        .doOnError(this::processError)
                        .retryWhen(retrySpec)
                        .repeat()
                        .publishOn(scheduler)
                        .doOnNext(this::updateAssignments)
                        .doOnEach(x -> shardAssignmentsEvents.increment())
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
                        final var ex = new NamespaceNotFoundException(assignments.getNamespace());
                        log.error("Failed receiving shard assignments", ex);
                        throw ex;
                    }
                }
            }
        }
        log.warn("Failed receiving shard assignments", error);
    }

    private void updateAssignments(io.streamnative.oxia.proto.ShardAssignments shardAssignments) {
        var nsSharedAssignments = shardAssignments.getNamespacesMap().get(assignments.getNamespace());
        if (nsSharedAssignments == null) {
            /*
            It shouldn't happen, but we have to do some defensive programming to avoid server nodes
            Allowing namespaces to be deleted in the future.

            Retries are available so that the client does not panic until the namespace is recreated.
            */
            throw new NamespaceNotFoundException(assignments.getNamespace(), true);
        }
        var updates =
                nsSharedAssignments.getAssignmentsList().stream().map(Shard::fromProto).collect(toSet());
        var updatedMap = recomputeShardHashBoundaries(assignments.allShards(), updates);
        var changes = computeShardLeaderChanges(assignments.allShards(), updatedMap);
        assignments.update(changes);
        callbacks.accept(changes);
    }

    @VisibleForTesting
    static Map<Long, Shard> recomputeShardHashBoundaries(
            Map<Long, Shard> assignments, Set<Shard> updates) {
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
        Set<Shard> removed =
                oldAssignments.values().stream()
                        .filter(shard -> !newAssignments.containsKey(shard.id()))
                        .collect(toSet());
        Set<Shard> added =
                newAssignments.values().stream()
                        .filter(shard -> !oldAssignments.containsKey(shard.id()))
                        .collect(toSet());
        Set<Shard> changed =
                oldAssignments.values().stream()
                        .filter(s -> newAssignments.containsKey(s.id()))
                        .filter(s -> !newAssignments.get(s.id()).leader().equals(s.leader()))
                        .collect(toSet());
        return new ShardAssignmentChanges(
                unmodifiableSet(added), unmodifiableSet(removed), unmodifiableSet(changed));
    }

    public record ShardAssignmentChanges(
            Set<Shard> added, Set<Shard> removed, Set<Shard> reassigned) {}

    public long getShardForKey(String key) {
        return assignments.getShardForKey(key);
    }

    public Collection<Shard> allShards() {
        return assignments.allShards().values();
    }

    public Set<Long> allShardIds() {
        return assignments.allShardIds();
    }

    public String leader(long shardId) {
        return assignments.leader(shardId);
    }

    public void addCallback(@NonNull Consumer<ShardAssignmentChanges> callback) {
        callbacks.add(callback);
    }

    private boolean isErrorRetryable(@NonNull Throwable ex) {
        if (ex instanceof NamespaceNotFoundException nsNotFoundError) {
            return nsNotFoundError.isRetryable();
        }
        // Allow the rest of the errors to retry.
        return true;
    }
}
