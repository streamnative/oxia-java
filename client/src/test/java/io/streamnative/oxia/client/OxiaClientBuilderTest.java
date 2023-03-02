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

import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class OxiaClientBuilderTest {

    OxiaClientBuilder builder = new OxiaClientBuilder("address:1234");

    @Test
    void requestTimeout() {
        assertThatThrownBy(() -> builder.requestTimeout(ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.requestTimeout(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.requestTimeout(Duration.ofMillis(1)));
    }

    @Test
    void batchLinger() {
        assertThatThrownBy(() -> builder.batchLinger(ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.batchLinger(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.batchLinger(Duration.ofMillis(1)));
    }

    @Test
    void maxRequestsPerBatch() {
        assertThatThrownBy(() -> builder.maxRequestsPerBatch(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxRequestsPerBatch(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.maxRequestsPerBatch(1));
    }

    @Test
    void maxBatchSize() {
        assertThatThrownBy(() -> builder.maxBatchSize(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxBatchSize(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.maxBatchSize(1));
    }

    @Test
    void operationQueueCapacity() {
        assertThatThrownBy(() -> builder.operationQueueCapacity(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.operationQueueCapacity(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.operationQueueCapacity(1));
    }

    @Test
    void recordCacheCapacity() {
        assertThatThrownBy(() -> builder.recordCacheCapacity(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.recordCacheCapacity(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.recordCacheCapacity(1));
    }

    @Test
    void sessionTimeout() {
        assertThatThrownBy(() -> builder.sessionTimeout(ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.sessionTimeout(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.sessionTimeout(Duration.ofMillis(1)));
    }
}
