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
package io.streamnative.oxia.client.notify;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor(access = PACKAGE)
@Slf4j
public class NotificationManager implements AutoCloseable {
    private final ConcurrentMap<Long, ShardNotificationReceiver> shardReceivers =
            new ConcurrentHashMap<>();
    private final ShardManager shardManager;
    private final Function<Long, ShardNotificationReceiver> recieverFactory;
    private final CompositeCallback compositeCallback;
    private final CompletableFuture<Void> started = new CompletableFuture<>();

    public NotificationManager(
            @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
            @NonNull ShardManager shardManager) {
        this.compositeCallback = new CompositeCallback();
        this.recieverFactory =
                s -> new ShardNotificationReceiver(() -> stubByShardId.apply(s), s, compositeCallback);
        this.shardManager = shardManager;
    }

    public NotificationManager(
            @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
            @NonNull ShardManager shardManager) {
        this.compositeCallback = new CompositeCallback();
        this.recieverFactory =
                s -> new ShardNotificationReceiver(() -> stubByShardId.apply(s), s, compositeCallback);
        this.shardManager = shardManager;
    }

    public CompletableFuture<Void> startIfRequired() {
        start().thenRun(() -> started.complete(null));
        return started;
    }

    CompletableFuture<Void> start() {
        shardReceivers.putAll(
                shardManager.getAll().parallelStream()
                        .map(recieverFactory::apply)
                        .collect(toMap(ShardNotificationReceiver::getShardId, Function.identity())));
        return CompletableFuture.allOf(
                shardReceivers.values().stream()
                        .map(GrpcResponseStream::start)
                        .collect(toList())
                        .toArray(new CompletableFuture[shardReceivers.size()]));
    }

    public void registerCallback(@NonNull Consumer<Notification> callback) {
        compositeCallback.add(callback);
    }

    @Override
    public void close() throws Exception {
        shardReceivers.values().parallelStream().forEach(GrpcResponseStream::close);
    }

    static class CompositeCallback implements Consumer<Notification> {
        final Set<Consumer<Notification>> callbacks = ConcurrentHashMap.newKeySet();

        void add(Consumer<Notification> callback) {
            callbacks.add(callback);
        }

        @Override
        public void accept(Notification notification) {
            callbacks.parallelStream().forEach(c -> c.accept(notification));
        }
    }
}
