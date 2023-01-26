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
package io.streamnative.oxia.client;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ProtoUtilTest {

    @Test
    void uint32ToLong() {
        assertThat(ProtoUtil.uint32ToLong(1)).isEqualTo(1L);
        assertThat(ProtoUtil.uint32ToLong(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
        assertThat(ProtoUtil.uint32ToLong(Integer.MIN_VALUE)).isEqualTo(Integer.MAX_VALUE + 1L);
        assertThat(ProtoUtil.uint32ToLong(Integer.MIN_VALUE + 1)).isEqualTo(Integer.MAX_VALUE + 2L);
        assertThat(ProtoUtil.uint32ToLong(-1)).isEqualTo(4294967295L);
        assertThat(ProtoUtil.uint32ToLong(0)).isEqualTo(0L);
    }

    @Test
    void longToUnit32() {
        assertThat(ProtoUtil.longToUint32(1L)).isEqualTo(1);
        assertThat(ProtoUtil.longToUint32(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
        assertThat(ProtoUtil.longToUint32(Integer.MAX_VALUE + 1L)).isEqualTo(Integer.MIN_VALUE);
        assertThat(ProtoUtil.longToUint32(Integer.MAX_VALUE + 2L)).isEqualTo(Integer.MIN_VALUE + 1);
        assertThat(ProtoUtil.longToUint32(4294967295L)).isEqualTo(-1);
        assertThat(ProtoUtil.longToUint32(0L)).isEqualTo(0);
    }
}
