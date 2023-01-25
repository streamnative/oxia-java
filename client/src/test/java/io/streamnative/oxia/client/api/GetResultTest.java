package io.streamnative.oxia.client.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import com.google.protobuf.ByteString;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.Stat;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class GetResultTest {

    @Test
    void fromProto() {
        var payload = "hello".getBytes(UTF_8);
        assertThat(GetResult.fromProto(
                GetResponse.newBuilder().setPayload(ByteString.copyFrom(payload)).
                        setStat(Stat.newBuilder().setVersion(1L).setCreatedTimestamp(2L).setModifiedTimestamp(3L)
                                .build()).build()
        )).isEqualTo(new GetResult(
                payload,
                new Version(
                        1L,
                        2L,
                        3L
                )
        ));
    }
}