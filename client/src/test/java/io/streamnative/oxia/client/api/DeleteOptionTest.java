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

import io.streamnative.oxia.client.DeleteOptionsUtil;
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
                        DeleteOptionsUtil.validate(
                                DeleteOption.ifVersionIdEquals(1L), DeleteOption.ifVersionIdEquals(1L)))
                .containsOnly(DeleteOption.ifVersionIdEquals(1L));
    }

    @Test
    void validateEmpty() {
        assertThat(DeleteOptionsUtil.validate()).containsOnly(DeleteOption.Unconditionally);
    }

    @Test
    void validateFail() {
        assertThatThrownBy(
                        () ->
                                DeleteOptionsUtil.validate(
                                        DeleteOption.Unconditionally, DeleteOption.ifVersionIdEquals(1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toVersionId() {
        assertThat(DeleteOptionsUtil.toVersionId(Set.of(DeleteOption.Unconditionally))).isEmpty();
        assertThat(DeleteOptionsUtil.toVersionId(Set.of(DeleteOption.ifVersionIdEquals(1L))))
                .hasValue(1L);
    }
}
