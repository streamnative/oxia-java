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
package io.streamnative.oxia.client.session;

import static java.util.Collections.unmodifiableMap;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionManager
        implements AutoCloseable, Consumer<ShardAssignmentChanges>, SessionNotificationListener {

    private final ConcurrentMap<Long, Session> sessionsByShardId = new ConcurrentHashMap<>();
    private final SessionFactory factory;
    private volatile boolean closed = false;

    public SessionManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull ClientConfig config,
            @NonNull Function<Long, OxiaStub> stubByShardId,
            @NonNull InstrumentProvider instrumentProvider) {
        this.factory = new SessionFactory(executor, config, this, stubByShardId, instrumentProvider);
    }

    SessionManager(SessionFactory factory) {
        this.factory = factory;
    }

    @NonNull
    public Session getSession(long shardId) {
        if (closed) {
            throw new IllegalStateException("session manager has been closed");
        }
        return sessionsByShardId.computeIfAbsent(shardId, s -> factory.create(shardId));
    }

    @Override
    public void onSessionClosed(Session session) {
        sessionsByShardId.remove(session.getShardId(), session);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        sessionsByShardId.entrySet().parallelStream().forEach(entry -> closeQuietly(entry.getValue()));
    }

    @VisibleForTesting
    Map<Long, Session> sessions() {
        return unmodifiableMap(new HashMap<>(sessionsByShardId));
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        if (!closed) {
            // Removed shards do not have any sessions to keep alive
            changes.removed().forEach(s -> closeQuietly(sessionsByShardId.remove(s.id())));
        }
    }

    @VisibleForTesting
    Optional<Session> closeQuietly(Session session) {
        if (session != null) {
            session.close();
        }
        return Optional.ofNullable(session);
    }
}
