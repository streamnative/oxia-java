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

import static io.streamnative.oxia.client.ProtoUtil.longToUint32;
import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PUBLIC;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.proto.CloseSessionRequest;
import io.streamnative.oxia.proto.CreateSessionRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import io.streamnative.oxia.proto.SessionHeartbeat;
import java.time.Duration;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@RequiredArgsConstructor(access = PACKAGE)
@Slf4j
public class Session implements AutoCloseable {

    private final @NonNull Function<Long, ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubByShardId;
    private final @NonNull Duration sessionTimeout;
    private final @NonNull Duration heartbeatInterval;
    private final long shardId;

    @Getter(PUBLIC)
    private final long sessionId;

    private Disposable keepAliveSubscription;

    Session(
            @NonNull Function<Long, ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubByShardId,
            @NonNull ClientConfig config,
            long shardId,
            long sessionId) {
        this(
                stubByShardId,
                config.sessionTimeout(),
                Duration.ofMillis(
                        Math.max(config.sessionTimeout().toMillis() / 10, Duration.ofSeconds(2).toMillis())),
                shardId,
                sessionId);
    }

    private final SessionHeartbeat heartbeat =
            SessionHeartbeat.newBuilder()
                    .setShardId(longToUint32(sessionId))
                    .setSessionId(sessionId)
                    .build();

    void start() {
        keepAliveSubscription =
                stubByShardId
                        .apply(shardId)
                        .keepAlive(Mono.just(heartbeat).repeat().delayElements(heartbeatInterval))
                        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100L)))
                        .timeout(sessionTimeout)
                        .doOnError(
                                t -> {
                                    log.error("Failed to keep-alive session: {}", sessionId, t);
                                })
                        .subscribe();
    }

    @Override
    public void close() throws Exception {
        keepAliveSubscription.dispose();
        var stub = stubByShardId.apply(shardId);
        var request =
                CloseSessionRequest.newBuilder()
                        .setShardId(longToUint32(shardId))
                        .setSessionId(sessionId)
                        .build();
        stub.closeSession(request).block();
    }

    @RequiredArgsConstructor(access = PACKAGE)
    static class Factory {
        @NonNull ClientConfig config;
        @NonNull Function<Long, ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubByShardId;

        @NonNull
        Session create(long shardId) {
            var stub = stubByShardId.apply(shardId);
            var request =
                    CreateSessionRequest.newBuilder()
                            .setSessionTimeoutMs(
                                    (int) Math.max(config.sessionTimeout().toMillis(), Integer.MAX_VALUE))
                            .setShardId(longToUint32(shardId))
                            .setClientIdentity(config.clientIdentifier())
                            .build();
            var response = stub.createSession(request).block();
            if (response == null) {
                throw new IllegalStateException("Empty session returned for shardId: " + shardId);
            }
            return new Session(stubByShardId, config, shardId, response.getSessionId());
        }
    }
}
