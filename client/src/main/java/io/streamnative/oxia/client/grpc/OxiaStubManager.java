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

import io.streamnative.oxia.client.api.Authentication;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

public class OxiaStubManager implements AutoCloseable {
    private final Map<String, OxiaStub> stubs = new ConcurrentHashMap<>();

    @Nullable private final Authentication authentication;

    public OxiaStubManager(@Nullable Authentication authentication) {
        this.authentication = authentication;
    }

    public OxiaStub getStub(String address) {
        return stubs.computeIfAbsent(address, addr -> new OxiaStub(addr, authentication));
    }

    @Override
    public void close() throws Exception {
        for (OxiaStub stub : stubs.values()) {
            stub.close();
        }
    }
}
