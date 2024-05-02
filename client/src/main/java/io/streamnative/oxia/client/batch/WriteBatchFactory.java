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
import io.streamnative.oxia.client.session.SessionManager;
import java.util.function.Function;
import lombok.NonNull;

class WriteBatchFactory extends BatchFactory {
    final @NonNull SessionManager sessionManager;

    final LatencyHistogram writeRequestLatencyHistogram;

    public WriteBatchFactory(
            @NonNull Function<Long, OxiaStub> stubByShardId,
            @NonNull SessionManager sessionManager,
            @NonNull ClientConfig config,
            @NonNull InstrumentProvider instrumentProvider) {
        super(stubByShardId, config);
        this.sessionManager = sessionManager;

        writeRequestLatencyHistogram =
                instrumentProvider.newLatencyHistogram(
                        "oxia.client.ops.req",
                        "The latency of a get batch request to the server",
                        Attributes.builder().put("oxia.batch.type", "write").build());
    }

    @Override
    public Batch getBatch(long shardId) {
        return new WriteBatch(
                this,
                stubByShardId,
                sessionManager,
                getConfig().clientIdentifier(),
                shardId,
                getConfig().maxBatchSize());
    }
}
