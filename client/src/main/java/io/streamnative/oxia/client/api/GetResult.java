package io.streamnative.oxia.client.api;


import io.streamnative.oxia.proto.GetResponse;
import lombok.NonNull;
import lombok.Value;

/**
 * The result of a client get request.
 *
 * @param payload The payload associated with the key specified in the call.
 * @param stat Metadata for the record associated with the key specified in the call.
 */
@Value
public class GetResult {
    byte @NonNull [] payload;
    @NonNull Stat stat;

    public static @NonNull GetResult fromProto(@NonNull GetResponse response) {
        return new GetResult(response.getPayload().toByteArray(), Stat.fromProto(response.getStat()));
    }
}
