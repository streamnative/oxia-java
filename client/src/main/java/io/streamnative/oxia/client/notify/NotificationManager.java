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

import static java.util.stream.Collectors.toSet;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.CompositeConsumer;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor(access = PACKAGE)
@Slf4j
public class NotificationManager implements AutoCloseable, Consumer<ShardManager.Assignments> {
    private final ConcurrentMap<Long, ShardNotificationReceiver> shardReceivers =
            new ConcurrentHashMap<>();
    private final Function<Long, ShardNotificationReceiver> recieverFactory;
    private final CompositeConsumer<Notification> compositeCallback;
    private volatile boolean closed = false;

    public NotificationManager(
            @NonNull Function<Long, ReactorOxiaClientStub> stubByShardId,
            @NonNull Function<Long, String> leaderByShardId) {
        this.compositeCallback = new CompositeConsumer<>();
        this.recieverFactory =
                s ->
                        new ShardNotificationReceiver(
                                () -> stubByShardId.apply(s), s, leaderByShardId.apply(s), compositeCallback);
    }

    @Override
    public void accept(ShardManager.Assignments assignments) {
        if (closed) {
            return;
        }
        List<Long> shards = assignments.getAll();

        Set<Long> removed =
                shardReceivers.keySet().stream().filter(s -> !shards.contains(s)).collect(toSet());
        Set<Long> added =
                shards.stream().filter(key -> !shardReceivers.containsKey(key)).collect(toSet());
        Set<Long> changed =
                shardReceivers.entrySet().stream()
                        .filter(e -> shards.contains(e.getKey()))
                        .filter(e -> !assignments.leader(e.getKey()).equals(e.getValue().getLeader()))
                        .map(Map.Entry::getKey)
                        .collect(toSet());

        removed.forEach(s -> shardReceivers.remove(s).close());
        added.forEach(s -> shardReceivers.computeIfAbsent(s, recieverFactory::apply).start());
        changed.forEach(
                s -> {
                    var receiver = shardReceivers.remove(s);
                    receiver.close();
                    shardReceivers.computeIfAbsent(s, recieverFactory::apply).start(receiver.getOffset());
                });
    }

    public void registerCallback(@NonNull Consumer<Notification> callback) {
        if (closed) {
            throw new IllegalStateException("|Notification manager has been closed");
        }
        compositeCallback.add(callback);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        shardReceivers.values().parallelStream().forEach(GrpcResponseStream::close);
    }
}
