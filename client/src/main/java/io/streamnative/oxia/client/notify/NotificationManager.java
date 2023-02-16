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
import static java.util.stream.Collectors.toSet;

import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class NotificationManager implements AutoCloseable {
    private final Set<Consumer<Notification>> callbacks = ConcurrentHashMap.newKeySet();
    private Set<ShardNotificationReceiver> shardReceivers = ConcurrentHashMap.newKeySet();
    private final Function<Long, ReactorOxiaClientStub> stubByShardId;
    private final ShardManager shardManager;
    private final Consumer<Notification> compositeCallback = new CompositeCallback();
    private volatile CompletableFuture<Void> started;

    public CompletableFuture<Void> startIfRequired() {
        if (started == null) {
            synchronized (this) {
                if (started == null) {
                    started = start();
                    return started;
                }
            }
        }
        return started;
    }

    CompletableFuture<Void> start() {
        shardReceivers.addAll(
                shardManager.getAll().parallelStream()
                        .map(
                                s ->
                                        new ShardNotificationReceiver(
                                                () -> stubByShardId.apply(s), s, compositeCallback))
                        .collect(toSet()));
        return CompletableFuture.allOf(
                shardReceivers.stream()
                        .map(GrpcResponseStream::start)
                        .collect(toList())
                        .toArray(new CompletableFuture[shardReceivers.size()]));
    }

    public void registerCallback(@NonNull Consumer<Notification> callback) {
        callbacks.add(callback);
    }

    @Override
    public void close() throws Exception {
        shardReceivers.parallelStream().forEach(GrpcResponseStream::close);
    }

    class CompositeCallback implements Consumer<Notification> {

        @Override
        public void accept(Notification notification) {
            callbacks.parallelStream().forEach(c -> c.accept(notification));
        }
    }
}
