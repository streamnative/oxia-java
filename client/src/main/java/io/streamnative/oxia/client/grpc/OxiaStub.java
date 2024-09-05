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

import io.grpc.CallCredentials;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.TlsChannelCredentials;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.streamnative.oxia.client.api.Authentication;
import io.streamnative.oxia.client.batch.WriteStreamWrapper;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import lombok.NonNull;

public class OxiaStub implements AutoCloseable {
    private final ManagedChannel channel;
    private final String namespace;

    private final @NonNull OxiaClientGrpc.OxiaClientStub asyncStub;

    private final Map<Long, WriteStreamWrapper> writeStreams = new ConcurrentHashMap<>();

    public OxiaStub(
            String address,
            String namespace,
            @Nullable Authentication authentication,
            boolean enableTls) {
        this(
                NettyChannelBuilder.forTarget(
                                address,
                                enableTls
                                        ? TlsChannelCredentials.newBuilder().build()
                                        : InsecureChannelCredentials.create())
                        .directExecutor()
                        .build(),
                namespace,
                authentication);
    }

    public OxiaStub(ManagedChannel channel, String namespace) {
        this(channel, namespace, null);
    }

    public OxiaStub(
            ManagedChannel channel, String namespace, @Nullable final Authentication authentication) {
        this.namespace = namespace;
        this.channel = channel;
        if (authentication != null) {
            this.asyncStub =
                    OxiaClientGrpc.newStub(channel)
                            .withCallCredentials(
                                    new CallCredentials() {

                                        @Override
                                        public void applyRequestMetadata(
                                                RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
                                            applier.apply(authentication.generateCredentials());
                                        }

                                        @Override
                                        public void thisUsesUnstableApi() {
                                            // Nothing to do.
                                        }
                                    });
        } else {
            this.asyncStub = OxiaClientGrpc.newStub(channel);
        }
    }

    public OxiaClientGrpc.OxiaClientStub async() {
        return asyncStub;
    }

    private static final Metadata.Key<String> NAMESPACE_KEY =
            Metadata.Key.of("namespace", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SHARD_ID_KEY =
            Metadata.Key.of("shard-id", Metadata.ASCII_STRING_MARSHALLER);

    public WriteStreamWrapper writeStream(long streamId) {
        return writeStreams.compute(
                streamId,
                (key, stream) -> {
                    if (stream == null || !stream.isValid()) {
                        Metadata headers = new Metadata();
                        headers.put(NAMESPACE_KEY, namespace);
                        headers.put(SHARD_ID_KEY, String.format("%d", streamId));

                        OxiaClientGrpc.OxiaClientStub stub =
                                asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
                        return new WriteStreamWrapper(stub);
                    }
                    return stream;
                });
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
