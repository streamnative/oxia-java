package io.streamnative.oxia.client.api;


import io.streamnative.oxia.proto.PutResponse;
import lombok.NonNull;

/**
 * The result of a client get request.
 *
 * @param version Metadata for the record associated with the key specified in the call.
 */
public record PutResult(@NonNull Version version) {
    public static @NonNull PutResult fromProto(@NonNull PutResponse response) {
        return new PutResult(Version.fromProto(response.getStat()));
    }
}
