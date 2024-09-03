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
package io.streamnative.oxia.client.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public interface AsyncLock {

    /** Represents the different status of a lock. */
    enum LockStatus {
        INIT,
        ACQUIRING,
        ACQUIRED,
        RELEASING,
        RELEASED;
    }

    LockStatus getStatus();

    /**
     * Asynchronously acquires the lock.
     *
     * @return a CompletableFuture that completes when the lock is acquired
     */
    CompletableFuture<Void> lock();

    /**
     * Tries to acquire the lock asynchronously.
     *
     * @return a CompletableFuture that completes when the lock is acquired or not. The future will
     *     complete exceptionally with a {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException.LockBusyException} if the lock is
     *     acquired by others, or with an {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException.IllegalLockStatusException} if the
     *     current lock status is not {@link LockStatus#INIT} or {@link LockStatus#RELEASED}.
     *     Additionally, the future will complete exceptionally with an {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException} in case of an unknown error.
     */
    CompletableFuture<Void> tryLock();

    /**
     * Asynchronously releases the lock.
     *
     * @return a CompletableFuture that completes when the lock is acquired or not. The future will
     *     complete exceptionally with an {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException.IllegalLockStatusException} if the
     *     current lock status is {@link LockStatus#INIT}. In the case of an unknown error, the future
     *     will complete exceptionally with an {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException}.
     */
    CompletableFuture<Void> unlock();

    /**
     * Asynchronously acquires the lock using a specified ExecutorService.
     *
     * @param executorService the ExecutorService to use for acquiring the lock
     * @return a CompletableFuture that completes when the lock is acquired
     */
    CompletableFuture<Void> lock(ExecutorService executorService);

    /**
     * Tries to acquire the lock asynchronously using a specified ExecutorService.
     *
     * @param executorService the ExecutorService to use for acquiring the lock
     * @return a CompletableFuture that completes when the lock is acquired or not. The future will
     *     complete exceptionally with a {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException.LockBusyException} if the lock is
     *     acquired by others, or with an {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException.IllegalLockStatusException} if the
     *     current lock status is not {@link LockStatus#INIT} or {@link LockStatus#RELEASED}.
     *     Additionally, the future will complete exceptionally with an {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException} in case of an unknown error.
     */
    CompletableFuture<Void> tryLock(ExecutorService executorService);

    /**
     * Asynchronously releases the lock using a specified ExecutorService.
     *
     * @param executorService the ExecutorService to use for releasing the lock
     * @return a CompletableFuture that completes when the lock is acquired or not. The future will
     *     complete exceptionally with an {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException.IllegalLockStatusException} if the
     *     current lock status is {@link LockStatus#INIT}. In the case of an unknown error, the future
     *     will complete exceptionally with an {@link
     *     io.streamnative.oxia.client.api.exceptions.LockException}.
     */
    CompletableFuture<Void> unlock(ExecutorService executorService);
}
