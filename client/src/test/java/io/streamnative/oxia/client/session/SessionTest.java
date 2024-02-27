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

import static io.streamnative.oxia.client.OxiaClientBuilder.DefaultNamespace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.SessionMetrics;
import io.streamnative.oxia.client.metrics.api.Metrics;
import io.streamnative.oxia.proto.CloseSessionRequest;
import io.streamnative.oxia.proto.CloseSessionResponse;
import io.streamnative.oxia.proto.KeepAliveResponse;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import io.streamnative.oxia.proto.SessionHeartbeat;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class SessionTest {

    Function<Long, OxiaStub> stubByShardId;
    ClientConfig config;
    long shardId = 1L;
    long sessionId = 2L;
    Duration sessionTimeout = Duration.ofSeconds(10);
    String clientId = "client";

    private Server server;
    private OxiaStub stub;
    private TestService service;

    @Mock SessionMetrics metrics;

    @BeforeEach
    void setup() throws IOException {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(3));

        config =
                new ClientConfig(
                        "address",
                        Duration.ZERO,
                        Duration.ZERO,
                        1,
                        1024 * 1024,
                        0,
                        sessionTimeout,
                        clientId,
                        Metrics.nullObject,
                        DefaultNamespace);

        String serverName = InProcessServerBuilder.generateName();
        service = new TestService();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(service)
                        .build()
                        .start();
        stub = new OxiaStub(InProcessChannelBuilder.forName(serverName).directExecutor().build());

        stubByShardId = s -> stub;
    }

    @AfterEach
    public void stopServer() throws Exception {
        server.shutdown();
        server.awaitTermination();
        stub.close();

        server = null;
        stub = null;
    }

    @Test
    void sessionId() {
        var session = new Session(stubByShardId, config, shardId, sessionId, metrics);
        assertThat(session.getShardId()).isEqualTo(shardId);
        assertThat(session.getSessionId()).isEqualTo(sessionId);
    }

    @Test
    void start() throws Exception {
        var session = new Session(stubByShardId, config, shardId, sessionId, metrics);
        session.start();

        await()
                .untilAsserted(
                        () -> {
                            assertThat(service.signals.size()).isGreaterThan(2);
                            assertThat(service.signals)
                                    .containsOnly(
                                            SessionHeartbeat.newBuilder()
                                                    .setSessionId(sessionId)
                                                    .setShardId(shardId)
                                                    .build());
                        });
        session.close();
        assertThat(service.closed).isTrue();
        assertThat(service.signalsAfterClosed).isEmpty();

        verify(metrics, atLeast(2)).recordKeepAlive(any(Signal.class));
    }

    static class TestService extends ReactorOxiaClientGrpc.OxiaClientImplBase {
        List<SessionHeartbeat> signals = new LinkedList<>();
        List<SessionHeartbeat> signalsAfterClosed = new LinkedList<>();
        AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public Mono<KeepAliveResponse> keepAlive(Mono<SessionHeartbeat> request) {
            return request.map(
                    heartbeat -> {
                        if (!closed.get()) {
                            signals.add(heartbeat);
                        } else {
                            signalsAfterClosed.add(heartbeat);
                        }
                        return KeepAliveResponse.getDefaultInstance();
                    });
        }

        @Override
        public Mono<CloseSessionResponse> closeSession(Mono<CloseSessionRequest> request) {
            closed.compareAndSet(false, true);
            return Mono.just(CloseSessionResponse.getDefaultInstance());
        }
    }
}
