package io.streamnative.oxia.client.api;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class VersionTest {

    @Test
    void valid() {
        assertThatNoException().isThrownBy(() -> new Version(0, 0, 0));
    }

    @Test
    void invalidCreated() {
        assertThatThrownBy(() -> new Version(0, -1, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidModified() {
        assertThatThrownBy(() -> new Version(0, 0, -1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidVersionId() {
        assertThatThrownBy(() -> new Version(-1, 0, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
