package io.streamnative.oxia.client.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import com.google.protobuf.ByteString;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.PutResponse;
import io.streamnative.oxia.proto.Stat;
import java.nio.charset.StandardCharsets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class PutResultTest {

    @Test
    void fromProto() {
        var payload = "hello".getBytes(UTF_8);
        assertThat(PutResult.fromProto(
                PutResponse.newBuilder().
                        setStat(Stat.newBuilder().setVersion(1L).setCreatedTimestamp(2L).setModifiedTimestamp(3L)
                                .build()).build()
        )).isEqualTo(new PutResult(
                new Version(
                        1L,
                        2L,
                        3L
                )
        ));
    }
}