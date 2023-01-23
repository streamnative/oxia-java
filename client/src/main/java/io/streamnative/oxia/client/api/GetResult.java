package io.streamnative.oxia.client.api;


import io.streamnative.oxia.proto.GetResponse;
import lombok.NonNull;

/**
 * The result of a client get request.
 *
 * @param payload The payload associated with the key specified in the call.
 * @param version Metadata for the record associated with the key specified in the call.
 */
public record GetResult(byte @NonNull [] payload, @NonNull Version version) {
    public static @NonNull GetResult fromProto(@NonNull GetResponse response) {
        return new GetResult(
                response.getPayload().toByteArray(), Version.fromProto(response.getStat()));
    }
}
