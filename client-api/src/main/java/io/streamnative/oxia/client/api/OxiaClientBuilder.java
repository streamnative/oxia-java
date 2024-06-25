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
package io.streamnative.oxia.client.api;

import io.opentelemetry.api.OpenTelemetry;
import io.streamnative.oxia.client.api.exceptions.OxiaException;
import io.streamnative.oxia.client.internal.DefaultImplementation;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface OxiaClientBuilder {

    static OxiaClientBuilder create(String serviceAddress) {
        return DefaultImplementation.getDefaultImplementation(serviceAddress);
    }

    SyncOxiaClient syncClient() throws OxiaException;

    CompletableFuture<AsyncOxiaClient> asyncClient();

    OxiaClientBuilder requestTimeout(Duration requestTimeout);

    OxiaClientBuilder batchLinger(Duration batchLinger);

    OxiaClientBuilder maxRequestsPerBatch(int maxRequestsPerBatch);

    OxiaClientBuilder namespace(String namespace);

    OxiaClientBuilder sessionTimeout(Duration sessionTimeout);

    OxiaClientBuilder clientIdentifier(String clientIdentifier);

    OxiaClientBuilder clientIdentifier(Supplier<String> clientIdentifier);

    OxiaClientBuilder openTelemetry(OpenTelemetry openTelemetry);

    OxiaClientBuilder authentication(Authentication authentication);
}
