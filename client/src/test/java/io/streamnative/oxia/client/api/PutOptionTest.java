package io.streamnative.oxia.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PutOptionTest {

    @Nested
    @DisplayName("ifVersionIdEquals tests")
    class IfVersionIdEqualsTests {
        @Test
        void ifVersionIdEquals() {
            assertThat(PutOption.ifVersionIdEquals(1L))
                    .satisfies(
                            o -> {
                                assertThat(o).isInstanceOf(PutOption.VersionIdPutOption.IfVersionIdEquals.class);
                                assertThat(o.toVersionId()).isEqualTo(1L);
                                assertThat(o.cannotCoExistWith(PutOption.Unconditionally)).isTrue();
                                assertThat(o.cannotCoExistWith(PutOption.IfRecordDoesNotExist)).isTrue();
                                assertThat(o.cannotCoExistWith(PutOption.EphemeralRecord)).isFalse();
                            });
        }

        @Test
        void versionIdLessThanZero() {
            assertThatNoException().isThrownBy(() -> PutOption.ifVersionIdEquals(0L));
            assertThatThrownBy(() -> PutOption.ifVersionIdEquals(-1L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("ifVersionIdEquals tests")
    class IfRecordDoesNotExistTests {
        @Test
        void ifRecordDoesNotExist() {
            assertThat(PutOption.IfRecordDoesNotExist)
                    .satisfies(
                            o -> {
                                assertThat(o).isInstanceOf(PutOption.VersionIdPutOption.IfRecordDoesNotExist.class);
                                assertThat(o.toVersionId()).isEqualTo(Version.KeyNotExists);
                                assertThat(o.cannotCoExistWith(PutOption.Unconditionally)).isTrue();
                                assertThat(o.cannotCoExistWith(PutOption.ifVersionIdEquals(1L))).isTrue();
                                assertThat(o.cannotCoExistWith(PutOption.EphemeralRecord)).isFalse();
                            });
        }
    }

    @Nested
    @DisplayName("Unconditionally tests")
    class UnconditionallyTests {
        @Test
        void unconditionally() {
            assertThat(PutOption.Unconditionally)
                    .satisfies(
                            o -> {
                                assertThat(o).isInstanceOf(PutOption.VersionIdPutOption.Unconditionally.class);
                                assertThat(o.toVersionId()).isNull();
                                assertThat(o.cannotCoExistWith(PutOption.IfRecordDoesNotExist)).isTrue();
                                assertThat(o.cannotCoExistWith(PutOption.ifVersionIdEquals(1L))).isTrue();
                                assertThat(o.cannotCoExistWith(PutOption.EphemeralRecord)).isFalse();
                            });
        }
    }

    @Nested
    @DisplayName("EphemeralRecord tests")
    class EphemeralRecordTests {
        @Test
        void ephemeral() {
            assertThat(PutOption.EphemeralRecord)
                    .satisfies(
                            o -> {
                                assertThat(o).isInstanceOf(PutOption.EphemeralRecord.class);
                                assertThat(o.cannotCoExistWith(PutOption.IfRecordDoesNotExist)).isFalse();
                                assertThat(o.cannotCoExistWith(PutOption.Unconditionally)).isFalse();
                                assertThat(o.cannotCoExistWith(PutOption.ifVersionIdEquals(1L))).isFalse();
                                assertThat(o.cannotCoExistWith(PutOption.EphemeralRecord)).isFalse();
                            });
        }
    }

    @Test
    void validate() {
        assertThat(
                        PutOption.validate(
                                PutOption.EphemeralRecord,
                                PutOption.EphemeralRecord,
                                PutOption.ifVersionIdEquals(1L),
                                PutOption.ifVersionIdEquals(1L)))
                .containsOnly(PutOption.EphemeralRecord, PutOption.ifVersionIdEquals(1L));
    }

    @Test
    void validateEmpty() {
        assertThat(PutOption.validate()).containsOnly(PutOption.Unconditionally);
    }

    @Test
    void validateFail() {
        assertThatThrownBy(
                        () ->
                                PutOption.validate(
                                        PutOption.Unconditionally,
                                        PutOption.IfRecordDoesNotExist,
                                        PutOption.ifVersionIdEquals(1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toVersionId() {
        assertThat(PutOption.toVersionId(Set.of(PutOption.EphemeralRecord))).isNull();
        assertThat(PutOption.toVersionId(Set.of(PutOption.Unconditionally))).isNull();
        assertThat(PutOption.toVersionId(Set.of(PutOption.IfRecordDoesNotExist)))
                .isEqualTo(Version.KeyNotExists);
        assertThat(PutOption.toVersionId(Set.of(PutOption.ifVersionIdEquals(1L)))).isEqualTo(1L);
        assertThat(
                        PutOption.toVersionId(
                                Set.of(PutOption.EphemeralRecord, PutOption.ifVersionIdEquals(1L))))
                .isEqualTo(1L);
    }

    @Test
    void toEphemeral() {
        assertThat(PutOption.toEphemeral(Set.of(PutOption.EphemeralRecord))).isTrue();
        assertThat(PutOption.toEphemeral(Set.of())).isFalse();
    }
}
