package io.streamnative.oxia.client.api;


import io.streamnative.oxia.proto.GetResponse;
import java.util.Arrays;
import lombok.NonNull;

/**
 * The result of a client get request.
 *
 * @param payload The payload associated with the key specified in the call.
 * @param stat Metadata for the record associated with the key specified in the call.
 */
public record GetResult(byte @NonNull [] payload, @NonNull Stat stat) {
    public static @NonNull GetResult fromProto(@NonNull GetResponse response) {
        return new GetResult(response.getPayload().toByteArray(), Stat.fromProto(response.getStat()));
    }

    // Recquired as records do not handle array equals
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetResult getResult = (GetResult) o;
        return Arrays.equals(payload, getResult.payload) && stat.equals(getResult.stat);
    }
}
