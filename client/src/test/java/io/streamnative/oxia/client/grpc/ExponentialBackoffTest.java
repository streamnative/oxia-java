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
