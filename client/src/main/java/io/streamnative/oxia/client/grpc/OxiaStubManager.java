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
package io.streamnative.oxia.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.internal.BackoffPolicy;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.api.Authentication;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;

public class OxiaStubManager implements AutoCloseable {
    @VisibleForTesting
    final Map<Key, OxiaStub> stubs = new ConcurrentHashMap<>();

    @Nullable private final Authentication authentication;
    private final boolean enableTls;
    @Nullable private final BackoffPolicy.Provider backoffProvider;

    private final int maxConnectionPerNode;

    public OxiaStubManager(
            @Nullable Authentication authentication,
            boolean enableTls,
            @Nullable BackoffPolicy.Provider backoffProvider,
            int maxConnectionPerNode) {
        this.authentication = authentication;
        this.enableTls = enableTls;
        this.backoffProvider = backoffProvider;
        this.maxConnectionPerNode = maxConnectionPerNode;
    }

    public OxiaStub getStub(String address) {
        final var random = ThreadLocalRandom.current().nextInt();
        var modKey = random % maxConnectionPerNode;
        if (modKey < 0) {
            modKey += maxConnectionPerNode;
        }
        return stubs.computeIfAbsent(
                new Key(address, modKey), key -> new OxiaStub(key.address, authentication, enableTls, backoffProvider));
    }

    @Override
    public void close() throws Exception {
        for (OxiaStub stub : stubs.values()) {
            stub.close();
        }
    }

    record Key(String address, int random){
    }
}
