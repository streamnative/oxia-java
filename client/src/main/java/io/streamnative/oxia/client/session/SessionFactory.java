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

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.proto.CreateSessionRequest;
import io.streamnative.oxia.proto.CreateSessionResponse;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
public class SessionFactory {
    @NonNull private final ScheduledExecutorService executor;
    @NonNull final ClientConfig config;

    @NonNull final SessionNotificationListener listener;

    @NonNull final Function<Long, OxiaStub> stubByShardId;

    @NonNull final InstrumentProvider instrumentProvider;

    @NonNull
    Session create(long shardId) {
        var stub = stubByShardId.apply(shardId);
        var request =
                CreateSessionRequest.newBuilder()
                        .setSessionTimeoutMs((int) config.sessionTimeout().toMillis())
                        .setShardId(shardId)
                        .setClientIdentity(config.clientIdentifier())
                        .build();
        CreateSessionResponse response = stub.blocking().createSession(request);
        return new Session(
                executor,
                stubByShardId,
                config,
                shardId,
                response.getSessionId(),
                instrumentProvider,
                listener);
    }
}
