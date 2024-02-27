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
import static lombok.AccessLevel.PACKAGE;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.SessionMetrics;
import io.streamnative.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class SessionManager implements AutoCloseable, Consumer<ShardAssignmentChanges> {

    private final ConcurrentMap<Long, Session> sessionsByShardId = new ConcurrentHashMap<>();
    private final @NonNull Session.Factory factory;
    private volatile boolean closed = false;

    public SessionManager(
            @NonNull ClientConfig config, @NonNull Function<Long, OxiaStub> stubByShardId) {
        this(new Session.Factory(config, stubByShardId, SessionMetrics.create(config.metrics())));
    }

    @NonNull
    public Session getSession(long shardId) {
        try {
            if (closed) {
                throw new IllegalStateException("session manager has been closed");
            }
            return sessionsByShardId.computeIfAbsent(
                    shardId,
                    s -> {
                        var session = factory.create(shardId);
                        session.start();
                        return session;
                    });
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        var closedSessions = new ArrayList<Session>();
        sessionsByShardId.entrySet().parallelStream()
                .forEach(entry -> closeQuietly(entry.getValue()).ifPresent(closedSessions::add));
        closedSessions.forEach(s -> sessionsByShardId.remove(s.getSessionId()));
    }

    @VisibleForTesting
    Map<Long, Session> sessions() {
        return unmodifiableMap(new HashMap<>(sessionsByShardId));
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        if (!closed) {
            // Added shards do not have any sessions to keep alive
            var removed = changes.removed();
            removed.forEach(s -> closeQuietly(sessionsByShardId.remove(s.shardId())));

            var reassigned = changes.reassigned();
            reassigned.forEach(
                    s ->
                            closeQuietly(sessionsByShardId.remove(s.shardId()))
                                    .ifPresent(c -> getSession(s.shardId())));
        }
    }

    @VisibleForTesting
    Optional<Session> closeQuietly(Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                log.warn("Error closing session {}", session.getSessionId(), e);
            }
        }
        return Optional.ofNullable(session);
    }
}
