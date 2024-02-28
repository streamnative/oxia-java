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
package io.streamnative.oxia.client.grpc;

import static lombok.AccessLevel.PROTECTED;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.Disposable;

@RequiredArgsConstructor(access = PROTECTED)
public abstract class GrpcResponseStream implements AutoCloseable {
    private final @NonNull OxiaStub stub;

    private volatile Disposable disposable;

    public @NonNull CompletableFuture<Void> start() {
        synchronized (this) {
            if (disposable != null) {
                throw new IllegalStateException("Already started");
            }
            return start(stub, disposable -> this.disposable = disposable);
        }
    }

    protected abstract @NonNull CompletableFuture<Void> start(
            @NonNull OxiaStub stub, @NonNull Consumer<Disposable> consumer);

    @Override
    public void close() {
        if (disposable != null) {
            synchronized (this) {
                if (disposable != null) {
                    disposable.dispose();
                    disposable = null;
                }
            }
        }
    }
}
