package io.streamnative.oxia.client.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Tests for the exponential backoff of retries")
class ExponentialBackoffTest {
    @Test
    void exponentialBackOffFn() {
        var fn = new ExponentialBackoff(() -> -10L, 100L, 1000L);
        assertThat(fn.apply(1)).isEqualTo(12L);
        assertThat(fn.apply(2)).isEqualTo(14L);
        assertThat(fn.apply(3)).isEqualTo(18L);
        assertThat(fn.apply(4)).isEqualTo(26L);
        assertThat(fn.apply(5)).isEqualTo(42L);
        assertThat(fn.apply(6)).isEqualTo(74L);
        assertThat(fn.apply(7)).isEqualTo(138L);
        assertThat(fn.apply(8)).isEqualTo(266L);
        assertThat(fn.apply(9)).isEqualTo(522L);
        assertThat(fn.apply(10)).isEqualTo(1000L);
        assertThat(fn.apply(11)).isEqualTo(1000L);
    }
}
