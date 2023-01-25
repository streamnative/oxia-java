package io.streamnative.oxia.client.api;


import io.streamnative.oxia.proto.PutResponse;
import lombok.NonNull;

/**
 * The result of a client get request.
 *
 * @param stat Metadata for the record associated with the key specified in the call.
 */
public record PutResult(@NonNull Stat stat) {
    public static @NonNull PutResult fromProto(@NonNull PutResponse response) {
        return new PutResult(Stat.fromProto(response.getStat()));
    }
}
