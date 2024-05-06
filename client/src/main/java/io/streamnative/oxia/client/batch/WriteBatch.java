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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.client.grpc.OxiaStubProvider;
import io.streamnative.oxia.client.session.SessionManager;
import io.streamnative.oxia.proto.WriteRequest;
import io.streamnative.oxia.proto.WriteResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;

final class WriteBatch extends BatchBase implements Batch, StreamObserver<WriteResponse> {

    private final WriteBatchFactory factory;

    @VisibleForTesting final List<Operation.WriteOperation.PutOperation> puts = new ArrayList<>();

    @VisibleForTesting
    final List<Operation.WriteOperation.DeleteOperation> deletes = new ArrayList<>();

    @VisibleForTesting
    final List<Operation.WriteOperation.DeleteRangeOperation> deleteRanges = new ArrayList<>();

    private final SessionManager sessionManager;
    private final String clientIdentifier;
    private final int maxBatchSize;
    private boolean containsEphemeral;
    private int byteSize;
    private long bytes;
    private long startSendTimeNanos;

    WriteBatch(
            @NonNull WriteBatchFactory factory,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull SessionManager sessionManager,
            @NonNull String clientIdentifier,
            long shardId,
            int maxBatchSize) {
        super(stubProvider, shardId);
        this.factory = factory;
        this.sessionManager = sessionManager;
        this.clientIdentifier = clientIdentifier;
        this.byteSize = 0;
        this.maxBatchSize = maxBatchSize;
    }

    int sizeOf(@NonNull Operation<?> operation) {
        if (operation instanceof Operation.WriteOperation.PutOperation p) {
            return p.key().getBytes(UTF_8).length + p.value().length;
        } else if (operation instanceof Operation.WriteOperation.DeleteOperation d) {
            return d.key().getBytes(UTF_8).length;
        } else if (operation instanceof Operation.WriteOperation.DeleteRangeOperation r) {
            return r.startKeyInclusive().getBytes(UTF_8).length
                    + r.endKeyExclusive().getBytes(UTF_8).length;
        }
        return 0;
    }

    public void add(@NonNull Operation<?> operation) {
        if (operation instanceof Operation.WriteOperation.PutOperation p) {
            puts.add(p);
            bytes += p.value().length;
            containsEphemeral |= p.ephemeral();
        } else if (operation instanceof Operation.WriteOperation.DeleteOperation d) {
            deletes.add(d);
        } else if (operation instanceof Operation.WriteOperation.DeleteRangeOperation r) {
            deleteRanges.add(r);
        }
        byteSize += sizeOf(operation);
    }

    @Override
    public boolean canAdd(@NonNull Operation<?> operation) {
        int size = sizeOf(operation);
        return byteSize + size <= maxBatchSize;
    }

    @Override
    public int size() {
        return puts.size() + deletes.size() + deleteRanges.size();
    }

    @Override
    public void send() {
        startSendTimeNanos = System.nanoTime();
        try {
            getStub().async().write(toProto(), this);
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void onNext(WriteResponse response) {
        factory.writeRequestLatencyHistogram.recordSuccess(System.nanoTime() - startSendTimeNanos);

        for (var i = 0; i < deletes.size(); i++) {
            deletes.get(i).complete(response.getDeletes(i));
        }
        for (var i = 0; i < deleteRanges.size(); i++) {
            deleteRanges.get(i).complete(response.getDeleteRanges(i));
        }
        for (var i = 0; i < puts.size(); i++) {
            puts.get(i).complete(response.getPuts(i));
        }
    }

    @Override
    public void onError(Throwable batchError) {
        factory.writeRequestLatencyHistogram.recordFailure(System.nanoTime() - startSendTimeNanos);
        deletes.forEach(d -> d.fail(batchError));
        deleteRanges.forEach(f -> f.fail(batchError));
        puts.forEach(p -> p.fail(batchError));
    }

    @Override
    public void onCompleted() {
        // Write is just single-rpc
    }

    @NonNull
    WriteRequest toProto() {
        Optional<Operation.WriteOperation.PutOperation.SessionInfo> sessionInfo;
        if (containsEphemeral) {
            sessionInfo =
                    Optional.of(
                            new Operation.WriteOperation.PutOperation.SessionInfo(
                                    sessionManager.getSession(getShardId()).getSessionId(), clientIdentifier));
        } else {
            sessionInfo = Optional.empty();
        }
        return WriteRequest.newBuilder()
                .setShardId(getShardId())
                .addAllPuts(puts.stream().map(p -> p.toProto(sessionInfo)).collect(toList()))
                .addAllDeletes(
                        deletes.stream()
                                .map(Operation.WriteOperation.DeleteOperation::toProto)
                                .collect(toList()))
                .addAllDeleteRanges(
                        deleteRanges.stream()
                                .map(Operation.WriteOperation.DeleteRangeOperation::toProto)
                                .collect(toList()))
                .build();
    }
}
