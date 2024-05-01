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
package io.streamnative.oxia.client.batch;

import io.opentelemetry.api.common.Attributes;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.client.metrics.LatencyHistogram;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;

class ReadBatchFactory extends BatchFactory {

    @Getter private final LatencyHistogram readRequestLatencyHistogram;

    public ReadBatchFactory(
            @NonNull Function<Long, OxiaStub> stubByShardId,
            @NonNull ClientConfig config,
            @NonNull InstrumentProvider instrumentProvider) {
        super(stubByShardId, config);

        readRequestLatencyHistogram =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops.req",
                        "The latency of a get batch request to the server",
                        Attributes.builder().put("oxia.batch.type", "read").build());
    }

    @Override
    public Batch getBatch(long shardId) {
        return new ReadBatch(this, stubByShardId, shardId);
    }
}
