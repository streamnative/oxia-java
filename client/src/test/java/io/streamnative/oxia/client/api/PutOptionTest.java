/*
 * Copyright © 2022-2024 StreamNative Inc.
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

import io.streamnative.oxia.client.PutOptionsUtil;
import java.util.Collections;
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
            assertThat(PutOption.IfVersionIdEquals(1L))
                    .satisfies(
                            o -> {
                                assertThat(o).isInstanceOf(OptionVersionId.OptionVersionIdEqual.class);

                                if (o instanceof OptionVersionId e) {
                                    assertThat(e.versionId()).isEqualTo(1);
                                }
                            });
        }

        @Test
        void versionIdLessThanZero() {
            assertThatNoException().isThrownBy(() -> PutOption.IfVersionIdEquals(0L));
            assertThatThrownBy(() -> PutOption.IfVersionIdEquals(-1L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("IfRecordDoesNotExist tests")
    class IfRecordDoesNotExistTests {
        @Test
        void ifRecordDoesNotExist() {
            assertThat(PutOption.IfRecordDoesNotExist)
                    .satisfies(
                            o -> {
                                assertThat(o).isInstanceOf(OptionVersionId.OptionRecordDoesNotExist.class);

                                if (o instanceof OptionVersionId e) {
                                    assertThat(e.versionId()).isEqualTo(Version.KeyNotExists);
                                }
                            });
        }
    }

    @Test
    void validateFail() {
        assertThatThrownBy(
                        () ->
                                PutOptionsUtil.getVersionId(
                                        Set.of(PutOption.IfRecordDoesNotExist, PutOption.IfVersionIdEquals(1L))))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                PutOptionsUtil.getVersionId(
                                        Set.of(PutOption.IfVersionIdEquals(1L), PutOption.IfVersionIdEquals(1L))))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                PutOptionsUtil.getVersionId(
                                        Set.of(PutOption.IfVersionIdEquals(1L), PutOption.IfVersionIdEquals(2L))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getVersionId() {
        assertThat(PutOptionsUtil.getVersionId(Set.of(PutOption.AsEphemeralRecord))).isEmpty();
        assertThat(PutOptionsUtil.getVersionId(Collections.emptySet())).isEmpty();
        assertThat(PutOptionsUtil.getVersionId(Set.of(PutOption.IfRecordDoesNotExist)))
                .hasValue(Version.KeyNotExists);
        assertThat(PutOptionsUtil.getVersionId(Set.of(PutOption.IfVersionIdEquals(1L)))).hasValue(1L);
        assertThat(
                        PutOptionsUtil.getVersionId(
                                Set.of(PutOption.AsEphemeralRecord, PutOption.IfVersionIdEquals(1L))))
                .hasValue(1L);
    }

    @Test
    void isEphemeral() {
        assertThat(PutOptionsUtil.isEphemeral(Set.of(PutOption.AsEphemeralRecord))).isTrue();
        assertThat(PutOptionsUtil.isEphemeral(Set.of(PutOption.IfRecordDoesNotExist))).isFalse();
        assertThat(PutOptionsUtil.isEphemeral(Set.of(PutOption.IfVersionIdEquals(5)))).isFalse();
        assertThat(PutOptionsUtil.isEphemeral(Collections.emptySet())).isFalse();
    }
}
