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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class VersionTest {

    @Test
    void valid() {
        assertThatNoException().isThrownBy(() -> new Version(0, 0, 0, 0));
    }

    @Test
    void invalidCreated() {
        assertThatThrownBy(() -> new Version(0, -1, 0, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidModified() {
        assertThatThrownBy(() -> new Version(0, 0, -1, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidVersionId() {
        assertThatThrownBy(() -> new Version(-1, 0, 0, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidModificationsCount() {
        assertThatThrownBy(() -> new Version(0, 0, 0, -1)).isInstanceOf(IllegalArgumentException.class);
    }
}
