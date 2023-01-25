package io.streamnative.oxia.client.api;


import io.streamnative.oxia.proto.GetResponse;
import lombok.NonNull;
import lombok.Value;

/** The result of a client get request. */
@Value
public class GetResult {
    /** The payload associated with the key specified in the call. */
    byte @NonNull [] payload;
    /** Metadata for the record associated with the key specified in the call. */
    @NonNull Stat stat;

    public static @NonNull GetResult fromProto(@NonNull GetResponse response) {
        return new GetResult(response.getPayload().toByteArray(), Stat.fromProto(response.getStat()));
    }
}
