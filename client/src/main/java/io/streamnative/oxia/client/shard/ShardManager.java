package io.streamnative.oxia.client.shard;

import static io.grpc.Status.fromThrowable;
import static io.streamnative.oxia.client.shard.HashRangeShardStrategy.Xxh332HashRangeShardStrategy;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PACKAGE;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientStub;
import io.streamnative.oxia.proto.ShardAssignmentsRequest;
import io.streamnative.oxia.proto.ShardAssignmentsResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor(access = PACKAGE)
@Slf4j
public class ShardManager implements AutoCloseable {
    private final Receiver receiver;
    private final Assignments assignments;

    ShardManager(String serviceAddress, Function<String, OxiaClientStub> clientSupplier) {
        this(Xxh332HashRangeShardStrategy, serviceAddress, clientSupplier);
    }

    @VisibleForTesting
    ShardManager(
            ShardStrategy strategy,
            String serviceAddress,
            Function<String, OxiaClientStub> clientSupplier) {
        assignments = new Assignments(strategy);
        receiver =
                new ReceiveWithRecovery(new GrpcReceiver(serviceAddress, clientSupplier, assignments));
    }

    CompletableFuture<Void> start() {
        receiver.receive();
        return receiver.bootstrap();
    }

    int get(String key) {
        return assignments.get(key);
    }

    List<Integer> getAll() {
        return assignments.getAll();
    }

    String leader(int shardId) {
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
        private Map<Integer, Shard> shards = new HashMap<>();
        private final ShardStrategy shardStrategy;

        Assignments(ShardStrategy shardStrategy) {
            this(new ReentrantReadWriteLock(), shardStrategy);
        }

        Assignments(ReadWriteLock lock, ShardStrategy shardStrategy) {
            this.shardStrategy = shardStrategy;
            rLock = lock.readLock();
            wLock = lock.writeLock();
        }

        public int get(String key) {
            try {
                rLock.lock();
                var test = shardStrategy.acceptsKeyPredicate(key);
                var shard = shards.values().stream().filter(test).findAny();
                return shard
                        .map(Shard::id)
                        .orElseThrow(() -> new IllegalStateException("shard not found for key: " + key));
            } finally {
                rLock.unlock();
            }
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
    }

    interface Receiver extends AutoCloseable {
        @NonNull
        CompletableFuture<Void> receive();

        @NonNull
        CompletableFuture<Void> bootstrap();
    }

    @RequiredArgsConstructor(access = PACKAGE)
    @VisibleForTesting
    static class ReceiveWithRecovery implements Receiver {
        private final ScheduledExecutorService executor;
        private final CompletableFuture<Void> closed;
        private final AtomicLong attemptCounter;
        private final LongFunction<Long> retryIntervalFn;
        private final Receiver receiver;

        ReceiveWithRecovery(@NonNull Receiver receiver) {
            this(
                    Executors.newSingleThreadScheduledExecutor(
                            r -> new Thread(r, "shard-manager-assignments-receiver")),
                    new CompletableFuture<>(),
                    new AtomicLong(),
                    new ExponentialBackoff(),
                    receiver);
        }

        @Override
        public @NonNull CompletableFuture<Void> receive() {
            executor.execute(this::receiveWithRetry);
            return closed;
        }

        @Override
        public @NonNull CompletableFuture<Void> bootstrap() {
            return receiver.bootstrap();
        }

        private void receiveWithRetry() {
            while (!closed.isDone()) {
                var attempt = attemptCounter.getAndIncrement();
                try {
                    if (attempt > 0) {
                        var interval = retryIntervalFn.apply(attempt);
                        Thread.sleep(interval);
                    }
                    receiver.receive().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    log.error("Shard assignments stream terminated", e.getCause());
                }
            }
        }

        @Override
        public void close() throws Exception {
            closed.complete(null);
            executor.shutdown();
        }

        @RequiredArgsConstructor(access = PACKAGE)
        @VisibleForTesting
        static class ExponentialBackoff implements LongFunction<Long> {
            private final Supplier<Long> randomLongSupplier;
            private final long maxRandom;
            private final long maxIntervalMs;

            ExponentialBackoff() {
                var random = new Random();
                randomLongSupplier = random::nextLong;
                this.maxRandom = 500L;
                this.maxIntervalMs = SECONDS.toMillis(30);
            }

            @Override
            public Long apply(long retryIndex) {
                return Math.min(
                        ((long) Math.pow(2.0, retryIndex)) + (Math.abs(randomLongSupplier.get()) % maxRandom),
                        maxIntervalMs);
            }
        }
    }

    @RequiredArgsConstructor(access = PACKAGE)
    @VisibleForTesting
    static class GrpcReceiver implements Receiver {
        private final String serviceAddress;
        private final Function<String, OxiaClientStub> clientSupplier;
        private final Assignments assignments;
        private final CompletableFuture<Void> bootstrap;
        private final Supplier<CompletableFuture<Void>> streamTerminalSupplier;

        GrpcReceiver(
                @NonNull String serviceAddress,
                @NonNull Function<String, OxiaClientStub> clientSupplier,
                @NonNull Assignments assignments) {
            this(
                    serviceAddress,
                    clientSupplier,
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
                System.out.println(observer);
                // Start the stream
                var client = clientSupplier.apply(serviceAddress);
                client.shardAssignments(ShardAssignmentsRequest.getDefaultInstance(), observer);
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
    static class ShardAssignmentsObserver implements StreamObserver<ShardAssignmentsResponse> {
        private final CompletableFuture<Void> streamTerminal;
        private final Consumer<ShardAssignmentsResponse> shardAssignmentsConsumer;

        @Override
        public void onNext(ShardAssignmentsResponse shardAssignments) {
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
