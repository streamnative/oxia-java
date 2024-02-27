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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc;
import lombok.NonNull;

public class OxiaStub implements AutoCloseable {
    private final ManagedChannel channel;

    private final @NonNull ReactorOxiaClientGrpc.ReactorOxiaClientStub reactorStub;

    private final @NonNull OxiaClientGrpc.OxiaClientBlockingStub blockingStub;

    public OxiaStub(String address) {
        this(
                Grpc.newChannelBuilder(address, InsecureChannelCredentials.create())
                .build()
        );
    }

    public OxiaStub(ManagedChannel channel) {
        this.channel = channel;
        this.reactorStub = ReactorOxiaClientGrpc.newReactorStub(channel);
        this.blockingStub = OxiaClientGrpc.newBlockingStub(channel);
    }

    public ReactorOxiaClientGrpc.ReactorOxiaClientStub reactor() {
        return reactorStub;
    }

    public OxiaClientGrpc.OxiaClientBlockingStub blocking() {
        return blockingStub;
    }

    @Override
    public void close() throws Exception {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(100, MILLISECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
        }
    }
}
