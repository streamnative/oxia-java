package io.streamnative.oxia.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DeleteOptionTest {

    @Nested
    @DisplayName("ifVersionIdEquals tests")
    class IfVersionIdEqualsTests {
        @Test
        void ifVersionIdEquals() {
            assertThat(DeleteOption.ifVersionIdEquals(1L))
                    .satisfies(
                            o -> {
                                assertThat(o)
                                        .isInstanceOf(DeleteOption.VersionIdDeleteOption.IfVersionIdEquals.class);
                                assertThat(o.toVersionId()).isEqualTo(1L);
                                assertThat(o.cannotCoExistWith(DeleteOption.Unconditionally)).isTrue();
                            });
        }

        @Test
        void versionIdLessThanZero() {
            assertThatNoException().isThrownBy(() -> DeleteOption.ifVersionIdEquals(0L));
            assertThatThrownBy(() -> DeleteOption.ifVersionIdEquals(-1L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("Unconditionally tests")
    class UnconditionallyTests {
        @Test
        void unconditionally() {
            assertThat(DeleteOption.Unconditionally)
                    .satisfies(
                            o -> {
                                assertThat(o)
                                        .isInstanceOf(DeleteOption.VersionIdDeleteOption.Unconditionally.class);
                                assertThat(o.toVersionId()).isNull();
                                assertThat(o.cannotCoExistWith(DeleteOption.ifVersionIdEquals(1L))).isTrue();
                            });
        }
    }

    @Test
    void validate() {
        assertThat(
                        DeleteOption.validate(
                                DeleteOption.ifVersionIdEquals(1L), DeleteOption.ifVersionIdEquals(1L)))
                .containsOnly(DeleteOption.ifVersionIdEquals(1L));
    }

    @Test
    void validateEmpty() {
        assertThat(DeleteOption.validate()).containsOnly(DeleteOption.Unconditionally);
    }

    @Test
    void validateFail() {
        assertThatThrownBy(
                        () ->
                                DeleteOption.validate(
                                        DeleteOption.Unconditionally, DeleteOption.ifVersionIdEquals(1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toVersionId() {
        assertThat(DeleteOption.toVersionId(Set.of(DeleteOption.Unconditionally))).isNull();
        assertThat(DeleteOption.toVersionId(Set.of(DeleteOption.ifVersionIdEquals(1L)))).isEqualTo(1L);
    }
}
