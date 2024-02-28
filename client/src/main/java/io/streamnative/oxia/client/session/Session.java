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

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PUBLIC;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.SessionMetrics;
import io.streamnative.oxia.proto.CloseSessionRequest;
import io.streamnative.oxia.proto.CreateSessionRequest;
import io.streamnative.oxia.proto.SessionHeartbeat;
import java.time.Duration;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

@RequiredArgsConstructor(access = PACKAGE)
@Slf4j
public class Session implements AutoCloseable {

    private final @NonNull Function<Long, OxiaStub> stubByShardId;
    private final @NonNull Duration sessionTimeout;
    private final @NonNull Duration heartbeatInterval;

    @Getter(PACKAGE)
    @VisibleForTesting
    private final long shardId;

    @Getter(PUBLIC)
    private final long sessionId;

    private final @NonNull SessionHeartbeat heartbeat;
    private final @NonNull SessionMetrics metrics;

    private Scheduler scheduler;
    private Disposable keepAliveSubscription;

    Session(
            @NonNull Function<Long, OxiaStub> stubByShardId,
            @NonNull ClientConfig config,
            long shardId,
            long sessionId,
            SessionMetrics metrics) {
        this(
                stubByShardId,
                config.sessionTimeout(),
                Duration.ofMillis(
                        Math.max(config.sessionTimeout().toMillis() / 10, Duration.ofSeconds(2).toMillis())),
                shardId,
                sessionId,
                SessionHeartbeat.newBuilder().setShardId(shardId).setSessionId(sessionId).build(),
                metrics);
        var threadName = String.format("session-[id=%s,shard=%s]-keep-alive", sessionId, shardId);
        scheduler = Schedulers.newSingle(threadName);
    }

    void start() {
        RetryBackoffSpec retrySpec =
                Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                        .doBeforeRetry(
                                signal ->
                                        log.warn(
                                                "Retrying sending keep-alives for session [id={},shard={}] - {}",
                                                sessionId,
                                                shardId,
                                                signal));
        keepAliveSubscription =
                Mono.just(heartbeat)
                        .repeat()
                        .delayElements(heartbeatInterval)
                        .flatMap(hb -> stubByShardId.apply(shardId).reactor().keepAlive(hb))
                        .retryWhen(retrySpec)
                        .timeout(sessionTimeout)
                        .publishOn(scheduler)
                        .doOnEach(metrics::recordKeepAlive)
                        .doOnError(
                                t -> log.warn("Session keep-alive error: [id={},shard={}]", sessionId, shardId, t))
                        .subscribe();
    }

    @Override
    public void close() throws Exception {
        keepAliveSubscription.dispose();
        var stub = stubByShardId.apply(shardId);
        var request =
                CloseSessionRequest.newBuilder().setShardId(shardId).setSessionId(sessionId).build();
        stub.reactor().closeSession(request).block();
        scheduler.dispose();
    }

    @RequiredArgsConstructor(access = PACKAGE)
    static class Factory {
        @NonNull ClientConfig config;
        @NonNull Function<Long, OxiaStub> stubByShardId;
        @NonNull SessionMetrics metrics;

        @NonNull
        Session create(long shardId) {
            var stub = stubByShardId.apply(shardId);
            var request =
                    CreateSessionRequest.newBuilder()
                            .setSessionTimeoutMs((int) config.sessionTimeout().toMillis())
                            .setShardId(shardId)
                            .setClientIdentity(config.clientIdentifier())
                            .build();
            var response = stub.reactor().createSession(request).block();
            if (response == null) {
                throw new IllegalStateException("Empty session returned for shardId: " + shardId);
            }
            return new Session(stubByShardId, config, shardId, response.getSessionId(), metrics);
        }
    }
}
