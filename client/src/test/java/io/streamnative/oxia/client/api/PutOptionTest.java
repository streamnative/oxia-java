/*
 * Copyright © 2022-2023 StreamNative Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
                                assertThat(o.cannotCoExistWith(PutOption.AsEphemeralRecord)).isFalse();
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
                                assertThat(o.cannotCoExistWith(PutOption.AsEphemeralRecord)).isFalse();
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
                                assertThat(o.cannotCoExistWith(PutOption.AsEphemeralRecord)).isFalse();
                            });
        }
    }

    @Nested
    @DisplayName("AsEphemeralRecord tests")
    class AsEphemeralRecordTests {
        @Test
        void asEphemeral() {
            assertThat(PutOption.AsEphemeralRecord)
                    .satisfies(
                            o -> {
                                assertThat(o).isInstanceOf(PutOption.AsEphemeralRecord.class);
                                assertThat(o.cannotCoExistWith(PutOption.IfRecordDoesNotExist)).isFalse();
                                assertThat(o.cannotCoExistWith(PutOption.Unconditionally)).isFalse();
                                assertThat(o.cannotCoExistWith(PutOption.ifVersionIdEquals(1L))).isFalse();
                                assertThat(o.cannotCoExistWith(PutOption.AsEphemeralRecord)).isFalse();
                            });
        }
    }

    @Test
    void validate() {
        assertThat(
                        PutOption.validate(
                                PutOption.AsEphemeralRecord,
                                PutOption.AsEphemeralRecord,
                                PutOption.ifVersionIdEquals(1L),
                                PutOption.ifVersionIdEquals(1L)))
                .containsOnly(PutOption.AsEphemeralRecord, PutOption.ifVersionIdEquals(1L));
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
        assertThat(PutOption.toVersionId(Set.of(PutOption.AsEphemeralRecord))).isEmpty();
        assertThat(PutOption.toVersionId(Set.of(PutOption.Unconditionally))).isEmpty();
        assertThat(PutOption.toVersionId(Set.of(PutOption.IfRecordDoesNotExist)))
                .hasValue(Version.KeyNotExists);
        assertThat(PutOption.toVersionId(Set.of(PutOption.ifVersionIdEquals(1L)))).hasValue(1L);
        assertThat(
                        PutOption.toVersionId(
                                Set.of(PutOption.AsEphemeralRecord, PutOption.ifVersionIdEquals(1L))))
                .hasValue(1L);
    }

    @Test
    void toEphemeral() {
        assertThat(PutOption.toEphemeral(Set.of(PutOption.AsEphemeralRecord))).isTrue();
        assertThat(PutOption.toEphemeral(Set.of())).isFalse();
    }
}
