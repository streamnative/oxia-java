package io.streamnative.oxia.client.shard;

import static io.grpc.Status.fromThrowable;
import static io.streamnative.oxia.client.shard.HashRangeShardStrategy.Xxh332HashRangeShardStrategy;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.client.shard.ShardManager.StreamTerminal.Complete;
import io.streamnative.oxia.client.shard.ShardManager.StreamTerminal.Error;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientStub;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import io.streamnative.oxia.proto.ShardAssignmentsResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ShardManager implements AutoCloseable {
    private final ShardStrategy shardStrategy;
    private final String serviceAddress;
    private final Function<String, OxiaClientStub> clientSuppler;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();
    private final CompletableFuture<Void> bootstrap = new CompletableFuture<>();
    private final BlockingQueue<StreamTerminal> streamTerminal = new ArrayBlockingQueue<>(1);
    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, "shard-manager-assignments-stream-observer"));
    private Map<Integer, Shard> shards = new HashMap<>();
    private volatile boolean closed = false;

    ShardManager(String serviceAddress, Function<String, OxiaClientStub> clientSuppler) {
        this(Xxh332HashRangeShardStrategy, serviceAddress, clientSuppler);
    }

    CompletableFuture<Void> start() {
        receiveWithRecovery();
        return bootstrap;
    }

    private void receiveWithRecovery() {
        executor.execute(
                () -> {
                    while (!closed) {
                        streamTerminal.clear();
                        receive();
                        try {
                            // Now block until the stream observer thread needs attention
                            var terminal = streamTerminal.take();
                            log.error("Shard assignments stream terminated: {}", terminal);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private void receive() {
        try {
            var observer =
                    new ShardAssignmentsObserver(
                            streamTerminal,
                            s -> {
                                update(
                                        s.getAssignmentsList().stream()
                                                .map(ShardConverter::fromProto)
                                                .collect(toList()));
                                // Signal to the manager that we have some initial shard assignments
                                bootstrap.complete(null);
                            });
            // Start the stream
            var client = clientSuppler.apply(serviceAddress);
            client.shardAssignments(
                    ShardAssignmentsRequest.newBuilder().getDefaultInstanceForType(), observer);
        } catch (Exception e) {
            bootstrap.completeExceptionally(e);
        }
    }

    public int get(String key) {
        var test = shardStrategy.acceptsKeyPredicate(key);
        var shard = shards.values().stream().filter(test).findAny();
        return shard
                .map(Shard::id)
                .orElseThrow(() -> new IllegalStateException("shard not found for key: " + key));
    }

    public List<Integer> getAll() {
        try {
            rLock.lock();
            return shards.keySet().stream().toList();
        } finally {
            rLock.unlock();
        }
    }

    public String leader(int shardId) {
        try {
            rLock.lock();
            return Optional.ofNullable(shards.get(shardId))
                    .map(Shard::leader)
                    .orElseThrow(() -> new IllegalStateException("shard not found for id: " + shardId));
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
    static Map<Integer, Shard> applyUpdates(Map<Integer, Shard> assignments, List<Shard> updates) {
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

    @Override
    public void close() throws Exception {
        closed = true;
        executor.shutdownNow();
    }

    @RequiredArgsConstructor
    static class ShardAssignmentsObserver implements StreamObserver<ShardAssignmentsResponse> {
        private final BlockingQueue<StreamTerminal> streamTerminal;
        private final Consumer<ShardAssignmentsResponse> shardAssignmentsConsumer;

        @Override
        public void onNext(ShardAssignmentsResponse shardAssignments) {
            shardAssignmentsConsumer.accept(shardAssignments);
        }

        @SneakyThrows
        @Override
        public void onError(Throwable throwable) {
            log.error("Failed receiving shard assignments - GRPC status: {}", fromThrowable(throwable));
            // Stream is broken, signal that recovery is necessary
            //noinspection ResultOfMethodCallIgnored
            streamTerminal.offer(new Error(throwable));
        }

        @SneakyThrows
        @Override
        public void onCompleted() {
            log.info("Shard Assignment stream completed.");
            // Stream is broken, signal that recovery is necessary
            //noinspection ResultOfMethodCallIgnored
            streamTerminal.offer(Complete.INSTANCE);
        }
    }

    sealed interface StreamTerminal permits Complete, Error {
        record Error(Throwable t) implements StreamTerminal {}

        enum Complete implements StreamTerminal {
            INSTANCE;
        }
    }
}
