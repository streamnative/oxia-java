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

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.proto.KeepAliveResponse;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import io.streamnative.oxia.proto.SessionHeartbeat;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class SessionTest {

    Function<Long, ReactorOxiaClientGrpc.ReactorOxiaClientStub> stubByShardId;
    ClientConfig config;
    long shardId = 1L;
    long sessionId = 2L;
    Duration sessionTimeout = Duration.ofMinutes(5);
    String clientId = "client";

    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void setup() throws IOException {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(3));

        config =
                new ClientConfig(
                        "address",
                        notification -> {},
                        Duration.ZERO,
                        Duration.ZERO,
                        1,
                        1,
                        sessionTimeout,
                        clientId);

        String serverName = InProcessServerBuilder.generateName();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(new TestService())
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        stubByShardId = s -> ReactorOxiaClientGrpc.newReactorStub(channel);
    }

    @AfterEach
    public void stopServer() throws InterruptedException {
        server.shutdown();
        server.awaitTermination();
        channel.shutdown();

        server = null;
        channel = null;
    }

    @Test
    void sessionId() {
        var session = new Session(stubByShardId, config, shardId, sessionId);
        assertThat(session.getSessionId()).isEqualTo(sessionId);
    }

    @Test
    void start() {
        //        var session = new Session(stubByShardId, config, shardId, sessionId);
        //        session.start();
        //
        //        Flux<SessionHeartbeat> req = Flux.just(
        //                SessionHeartbeat.newBuilder()
        //                        .setShardId(longToUint32(shardId))
        //                        .setSessionId(sessionId)
        //                        .build())
        //                .repeat(3);
        //
        //        Mono<KeepAliveResponse> resp = req.as(stub::sayHelloReqStream);
        //
        ////        ReactorGreeterGrpc.ReactorGreeterStub stub =
        // ReactorGreeterGrpc.newReactorStub(channel);
        ////        Flux<HelloRequest> req = Flux.just(
        ////                HelloRequest.newBuilder().setName("a").build(),
        ////                HelloRequest.newBuilder().setName("b").build(),
        ////                HelloRequest.newBuilder().setName("c").build());
        ////
        ////        if (!expectFusion) {
        ////            req = req.hide();
        ////        }
        ////
        ////        Mono<HelloResponse> resp = req.as(stub::sayHelloReqStream);
        ////        Mono<HelloResponse> resp = req.as(stub::sayHelloReqStream);
        ////
        ////        StepVerifier.Step<String> stepVerifier =
        // StepVerifier.create(resp.map(HelloResponse::getMessage));
        ////
        ////        if (expectFusion) {
        ////            stepVerifier = ((StepVerifier.FirstStep<String>) stepVerifier).expectFusion();
        ////        }
        ////
        ////        stepVerifier
        ////                .expectNext("Hello a and b and c")
        ////                .verifyComplete();
        //
        //
        //        Flux<KeepAliveResponse> resp = req.transform(r ->
        // stubByShardId.apply(shardId).keepAlive(r));
        //
        // StepVerifier.create(resp).expectNext(KeepAliveResponse.getDefaultInstance()).verifyComplete();
    }

    static class TestService extends ReactorOxiaClientGrpc.OxiaClientImplBase {
        List<Signal<SessionHeartbeat>> signals;

        @Override
        public Mono<KeepAliveResponse> keepAlive(Flux<SessionHeartbeat> request) {
            return request
                    .hide()
                    .doOnEach(h -> signals.add(h))
                    .hide()
                    .then()
                    .map(v -> KeepAliveResponse.getDefaultInstance());
        }
    }
}
