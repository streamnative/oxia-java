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
package io.streamnative.oxia.client;

import static java.time.Duration.ZERO;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.metrics.api.Metrics;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OxiaClientBuilder {

    public static final Duration DefaultBatchLinger = Duration.ofMillis(5);
    public static final int DefaultMaxRequestsPerBatch = 1000;
    public static final Duration DefaultRequestTimeout = Duration.ofSeconds(30);
    public static final int DefaultOperationQueueCapacity = 1000;
    public static final Duration DefaultSessionTimeout = Duration.ofSeconds(15);

    @NonNull private final String serviceAddress;
    @NonNull private Duration requestTimeout = DefaultRequestTimeout;
    @NonNull private Duration batchLinger = DefaultBatchLinger;
    private int maxRequestsPerBatch = DefaultMaxRequestsPerBatch;
    private int operationQueueCapacity = DefaultOperationQueueCapacity;
    @NonNull private Duration sessionTimeout = DefaultSessionTimeout;
    @NonNull private Supplier<String> clientIdentifier = OxiaClientBuilder::randomClientIdentifier;
    @NonNull private Metrics metrics = Metrics.nullObject;

    public @NonNull OxiaClientBuilder requestTimeout(@NonNull Duration requestTimeout) {
        if (requestTimeout.isNegative() || requestTimeout.equals(ZERO)) {
            throw new IllegalArgumentException(
                    "requestTimeout must be greater than zero: " + requestTimeout);
        }
        this.requestTimeout = requestTimeout;
        return this;
    }

    public @NonNull OxiaClientBuilder batchLinger(@NonNull Duration batchLinger) {
        if (batchLinger.isNegative() || batchLinger.equals(ZERO)) {
            throw new IllegalArgumentException("batchLinger must be greater than zero: " + batchLinger);
        }
        this.batchLinger = batchLinger;
        return this;
    }

    public @NonNull OxiaClientBuilder maxRequestsPerBatch(int maxRequestsPerBatch) {
        if (maxRequestsPerBatch <= 0) {
            throw new IllegalArgumentException(
                    "MaxRequestsPerBatch must be greater than zero: " + maxRequestsPerBatch);
        }
        this.maxRequestsPerBatch = maxRequestsPerBatch;
        return this;
    }

    public @NonNull OxiaClientBuilder operationQueueCapacity(int operationQueueCapacity) {
        if (operationQueueCapacity <= 0) {
            throw new IllegalArgumentException(
                    "operationQueueCapacity must be greater than zero: " + operationQueueCapacity);
        }
        this.operationQueueCapacity = operationQueueCapacity;
        return this;
    }

    public @NonNull OxiaClientBuilder sessionTimeout(@NonNull Duration sessionTimeout) {
        if (sessionTimeout.isNegative() || sessionTimeout.equals(ZERO)) {
            throw new IllegalArgumentException(
                    "SessionTimeout must be greater than zero: " + sessionTimeout);
        }
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    public @NonNull OxiaClientBuilder clientIdentifier(@NonNull String clientIdentifier) {
        this.clientIdentifier = () -> clientIdentifier;
        return this;
    }

    public @NonNull OxiaClientBuilder clientIdentifier(@NonNull Supplier<String> clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
        return this;
    }

    public @NonNull OxiaClientBuilder metrics(@NonNull Metrics metrics) {
        this.metrics = metrics;
        return this;
    }

    public @NonNull CompletableFuture<AsyncOxiaClient> asyncClient() {
        var config =
                new ClientConfig(
                        serviceAddress,
                        requestTimeout,
                        batchLinger,
                        maxRequestsPerBatch,
                        operationQueueCapacity,
                        sessionTimeout,
                        clientIdentifier.get(),
                        metrics);

        return AsyncOxiaClientImpl.newInstance(config);
    }

    public @NonNull SyncOxiaClient syncClient() {
        return new SyncOxiaClientImpl(asyncClient().join());
    }

    public static @NonNull String randomClientIdentifier() {
        return "oxia-client-java:" + UUID.randomUUID();
    }
}
