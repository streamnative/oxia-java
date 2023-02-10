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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ChannelManager implements Function<String, Channel>, AutoCloseable {
    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    @Getter private final @NonNull StubFactory<ReactorOxiaClientStub> reactorStubFactory;

    public ChannelManager() {
        reactorStubFactory = reactorStubFactory(this);
    }

    @Override
    public void close() throws Exception {
        channels.values().forEach(this::shutdown);
    }

    private void shutdown(ManagedChannel channel) {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(100, MILLISECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
        }
    }

    @Override
    public @NonNull Channel apply(@NonNull String address) {
        var serviceAddress = new ServiceAddress(address);
        return channels.computeIfAbsent(
                address,
                a ->
                        ManagedChannelBuilder.forAddress(serviceAddress.host(), serviceAddress.port())
                                .usePlaintext()
                                .build());
    }

    static StubFactory<ReactorOxiaClientStub> reactorStubFactory(
            @NonNull ChannelManager channelManager) {
        return new StubFactory<>(channelManager, ReactorOxiaClientGrpc::newReactorStub);
    }

    public static class StubFactory<T> implements Function<String, T> {
        private final Function<String, T> addressToStubFn;

        StubFactory(
                @NonNull ChannelManager channelManager, @NonNull Function<Channel, T> channelToStubFn) {
            addressToStubFn = channelManager.andThen(channelToStubFn);
        }

        private final ConcurrentMap<String, T> stubs = new ConcurrentHashMap<>();

        @Override
        public T apply(String address) {
            return stubs.computeIfAbsent(address, addressToStubFn);
        }
    }
}
