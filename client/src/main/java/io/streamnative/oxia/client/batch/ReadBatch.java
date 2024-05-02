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

import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.ReadRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import lombok.NonNull;
import reactor.core.publisher.Flux;

final class ReadBatch extends BatchBase implements Batch {

    private final ReadBatchFactory factory;

    @VisibleForTesting final List<Operation.ReadOperation.GetOperation> gets = new ArrayList<>();

    ReadBatch(ReadBatchFactory factory, Function<Long, OxiaStub> stubByShardId, long shardId) {
        super(stubByShardId, shardId);
        this.factory = factory;
    }

    @Override
    public boolean canAdd(@NonNull Operation<?> operation) {
        return true;
    }

    public void add(@NonNull Operation<?> operation) {
        if (operation instanceof Operation.ReadOperation.GetOperation g) {
            gets.add(g);
        }
    }

    @Override
    public int size() {
        return gets.size();
    }

    @Override
    public void send() {
        long startSendTimeNanos = System.nanoTime();

        LongAdder bytes = new LongAdder();
        try {
            var responses =
                    getStub()
                            .reactor()
                            .read(toProto())
                            .flatMapSequential(response -> Flux.fromIterable(response.getGetsList()))
                            .doOnNext(r -> bytes.add(r.getValue().size()));
            Flux.fromIterable(gets).zipWith(responses, this::complete).then().block();
            factory
                    .getReadRequestLatencyHistogram()
                    .recordSuccess(System.nanoTime() - startSendTimeNanos);
        } catch (Throwable batchError) {
            gets.forEach(g -> g.fail(batchError));
            factory
                    .getReadRequestLatencyHistogram()
                    .recordFailure(System.nanoTime() - startSendTimeNanos);
        }
    }

    private boolean complete(Operation.ReadOperation.GetOperation operation, GetResponse response) {
        operation.complete(response);
        return true;
    }

    @NonNull
    ReadRequest toProto() {
        return ReadRequest.newBuilder()
                .setShardId(getShardId())
                .addAllGets(
                        gets.stream().map(Operation.ReadOperation.GetOperation::toProto).collect(toList()))
                .build();
    }
}
