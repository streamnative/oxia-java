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
package io.streamnative.oxia.client.notify;

import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.CompositeConsumer;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.client.grpc.OxiaStubManager;
import io.streamnative.oxia.client.metrics.NotificationMetrics;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChange.Added;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor(access = PACKAGE)
@Slf4j
public class NotificationManager implements AutoCloseable, Consumer<ShardAssignmentChanges> {
    private final ConcurrentMap<Long, ShardNotificationReceiver> shardReceivers =
            new ConcurrentHashMap<>();
    private final @NonNull ShardNotificationReceiver.Factory recieverFactory;
    private final @NonNull ShardManager shardManager;
    private final @NonNull CompositeConsumer<Notification> compositeCallback;
    private final @NonNull NotificationMetrics metrics;
    private volatile boolean started = false;
    private volatile boolean closed = false;

    public NotificationManager(
            @NonNull OxiaStubManager stubManager,
            @NonNull ShardManager shardManager,
            @NonNull Metrics metrics) {
        this.compositeCallback = new CompositeConsumer<>();
        this.recieverFactory = new ShardNotificationReceiver.Factory(stubManager, compositeCallback);
        this.shardManager = shardManager;
        this.metrics = NotificationMetrics.create(metrics);
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        if (!started || closed) {
            return;
        }
        connectNotificationReceivers(changes);
    }

    public void registerCallback(@NonNull Consumer<Notification> callback) {
        if (closed) {
            throw new IllegalStateException("Notification manager has been closed");
        }
        compositeCallback.add(callback);
        if (!started) {
            synchronized (this) {
                if (!started) {
                    bootstrap();
                    started = true;
                }
            }
        }
    }

    private void bootstrap() {
        connectNotificationReceivers(
                new ShardAssignmentChanges(
                        shardManager.getAll().stream()
                                .map(s -> new Added(s, shardManager.leader(s)))
                                .collect(Collectors.toSet()),
                        Set.of(),
                        Set.of()));
    }

    private void connectNotificationReceivers(@NonNull ShardAssignmentChanges changes) {
        changes.removed().forEach(s -> shardReceivers.remove(s.shardId()).close());
        changes
                .added()
                .forEach(
                        s ->
                                shardReceivers
                                        .computeIfAbsent(
                                                s.shardId(), id -> recieverFactory.newReceiver(id, s.leader(), metrics))
                                        .start());
        changes
                .reassigned()
                .forEach(
                        s -> {
                            var receiver = Optional.ofNullable(shardReceivers.remove(s.shardId()));
                            receiver.ifPresent(GrpcResponseStream::close);
                            shardReceivers
                                    .computeIfAbsent(
                                            s.shardId(), id -> recieverFactory.newReceiver(id, s.toLeader(), metrics))
                                    .start(receiver.map(ShardNotificationReceiver::getOffset));
                        });
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        shardReceivers.values().parallelStream().forEach(GrpcResponseStream::close);
    }
}
