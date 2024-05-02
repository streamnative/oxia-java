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
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.ReadRequest;
import io.streamnative.oxia.proto.ReadResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.NonNull;

final class ReadBatch extends BatchBase implements Batch, StreamObserver<ReadResponse> {

    private final ReadBatchFactory factory;

    @VisibleForTesting final List<Operation.ReadOperation.GetOperation> gets = new ArrayList<>();

    private int responseIndex = 0;
    long startSendTimeNanos;

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
        startSendTimeNanos = System.nanoTime();
        try {
            getStub().async().read(toProto(), this);
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void onNext(ReadResponse response) {
        for (int i = 0; i < response.getGetsCount(); i++) {
            GetResponse gr = response.getGets(i);
            gets.get(responseIndex).complete(gr);

            ++responseIndex;
        }
    }

    @Override
    public void onError(Throwable batchError) {
        gets.forEach(g -> g.fail(batchError));
        factory.getReadRequestLatencyHistogram().recordFailure(System.nanoTime() - startSendTimeNanos);
    }

    @Override
    public void onCompleted() {
        factory.getReadRequestLatencyHistogram().recordSuccess(System.nanoTime() - startSendTimeNanos);
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
