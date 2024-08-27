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

import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import io.streamnative.oxia.proto.WriteRequest;
import io.streamnative.oxia.proto.WriteResponse;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WriteStreamWrapper {

    private final StreamObserver<WriteRequest> clientStream;

    private final ArrayDeque<CompletableFuture<WriteResponse>> pendingWrites = new ArrayDeque<>();

    private volatile Throwable failed = null;

    public WriteStreamWrapper(OxiaClientGrpc.OxiaClientStub stub) {
        this.clientStream = stub.writeStream(new StreamObserver<>() {
            @Override
            public void onNext(WriteResponse value) {
                synchronized (WriteStreamWrapper.this) {
                    var future = pendingWrites.poll();
                    if (future != null) {
                        future.complete(value);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                synchronized (WriteStreamWrapper.this) {
                    if (!pendingWrites.isEmpty()) {
                        log.warn("Got Error", t);
                    }
                    pendingWrites.forEach(f -> f.completeExceptionally(t));
                    pendingWrites.clear();
                    failed = t;
                }
            }

            @Override
            public void onCompleted() {
            }
        });
    }

    public synchronized CompletableFuture<WriteResponse> send(WriteRequest request) {
        if (failed != null) {
            return CompletableFuture.failedFuture(failed);
        }

        CompletableFuture<WriteResponse> future = new CompletableFuture<>();

        try {
            if (log.isDebugEnabled()) {
                log.debug("Sending request {}", request);
            }
            clientStream.onNext(request);
            pendingWrites.add(future);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    public boolean isValid() {
        return failed == null;
    }
}
