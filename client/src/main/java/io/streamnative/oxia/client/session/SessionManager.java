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
package io.streamnative.oxia.client.session;

import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class SessionManager implements AutoCloseable {

    private final ConcurrentMap<Long, Session> sessionsByShardId = new ConcurrentHashMap<>();
    private final @NonNull Session.Factory factory;

    public SessionManager(
            @NonNull ClientConfig config,
            @NonNull Function<Long, ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubByShardId) {
        this(new Session.Factory(config, stubByShardId));
    }

    @NonNull
    public Session getSession(long shardId) {
        return sessionsByShardId.computeIfAbsent(
                shardId,
                s -> {
                    var session = factory.create(shardId);
                    session.start();
                    return session;
                });
    }

    @Override
    public void close() throws Exception {
        var closed = new ArrayList<>();
        sessionsByShardId.values().parallelStream()
                .forEach(
                        s -> {
                            try {
                                s.close();
                                closed.add(s);
                            } catch (Exception e) {
                                log.error("Error closing session {}", s.getSessionId(), e);
                            }
                        });
        closed.forEach(sessionsByShardId::remove);
    }
}
