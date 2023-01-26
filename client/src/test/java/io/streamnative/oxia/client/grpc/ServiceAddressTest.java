package io.streamnative.oxia.client.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ServiceAddressTest {

    @Test
    void address() {
        var address = new ServiceAddress("a:10");
        assertThat(address.host()).isEqualTo("a");
        assertThat(address.port()).isEqualTo(10);
    }
}
