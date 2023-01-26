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
package io.streamnative.oxia.client.grpc;

import static io.streamnative.oxia.proto.OxiaClientGrpc.newBlockingStub;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static lombok.AccessLevel.PACKAGE;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientStub;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ChannelManager implements Function<String, ManagedChannel>, AutoCloseable {
    @NonNull ClientConfig config;
    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    @Getter private final @NonNull StubFactory stubFactory;
    @Getter private final @NonNull BlockingStubFactory blockingStubFactory;

    public ChannelManager(@NonNull ClientConfig config) {
        this.config = config;
        stubFactory = new StubFactory(this);
        blockingStubFactory = new BlockingStubFactory(this);
    }

    @Override
    public void close() throws Exception {
        channels.values().forEach(ManagedChannel::shutdown);
    }

    @Override
    public @NonNull ManagedChannel apply(@NonNull String address) {
        var serviceAddress = new ServiceAddress(address);
        return channels.computeIfAbsent(
                address,
                a ->
                        ManagedChannelBuilder.forAddress(serviceAddress.host(), serviceAddress.port())
                                .usePlaintext()
                                .keepAliveTimeout(config.requestTimeout().toMillis(), MILLISECONDS)
                                .build());
    }

    @RequiredArgsConstructor(access = PACKAGE)
    public static class StubFactory implements Function<String, OxiaClientStub> {
        private final @NonNull ChannelManager channelManager;
        private final ConcurrentMap<String, OxiaClientStub> stubs = new ConcurrentHashMap<>();

        @Override
        public @NonNull OxiaClientStub apply(@NonNull String address) {
            return stubs.computeIfAbsent(address, a -> OxiaClientGrpc.newStub(channelManager.apply(a)));
        }
    }

    @RequiredArgsConstructor(access = PACKAGE)
    public static class BlockingStubFactory implements Function<String, OxiaClientBlockingStub> {
        private final @NonNull ChannelManager channelManager;
        private final ConcurrentMap<String, OxiaClientBlockingStub> stubs = new ConcurrentHashMap<>();

        @Override
        public @NonNull OxiaClientBlockingStub apply(@NonNull String address) {
            return stubs.computeIfAbsent(address, a -> newBlockingStub(channelManager.apply(a)));
        }
    }
}
