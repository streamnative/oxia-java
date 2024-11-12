package io.streamnative.oxia.client.grpc;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class OxiaWriteStreamManager {
    private final Map<Long, WriteStreamWrapper> writeStreams = new ConcurrentHashMap<>();
    private final OxiaStubProvider provider;


    OxiaWriteStreamManager(OxiaStubProvider provider) {
        this.provider = provider;
    }

    private static final Metadata.Key<String> NAMESPACE_KEY =
            Metadata.Key.of("namespace", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SHARD_ID_KEY =
            Metadata.Key.of("shard-id", Metadata.ASCII_STRING_MARSHALLER);


    public WriteStreamWrapper writeStream(long shardId) {
        return writeStreams.compute(
                shardId,
                (key, stream) -> {
                    if (stream == null || !stream.isValid()) {
                        Metadata headers = new Metadata();
                        headers.put(NAMESPACE_KEY, provider.getNamespace());
                        headers.put(SHARD_ID_KEY, String.format("%d", shardId));
                        final var asyncStub = provider.getStubForShard(shardId).async();
                        return new WriteStreamWrapper(asyncStub
                                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers)));
                    }
                    return stream;
                });
    }
}
