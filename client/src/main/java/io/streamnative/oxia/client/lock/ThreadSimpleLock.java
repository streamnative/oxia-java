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
package io.streamnative.oxia.client.lock;

import io.streamnative.oxia.client.api.AsyncLock;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.OptionAutoRevalidate;
import io.streamnative.oxia.client.api.exceptions.LockException;
import io.streamnative.oxia.client.util.Backoff;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

final class ThreadSimpleLock implements AsyncLock, NotificationReceiver {
    private final SharedSimpleLock distributedLock;
    private final Semaphore memorySemaphore;

    @SafeVarargs
    public ThreadSimpleLock(
            AsyncOxiaClient client,
            String key,
            ScheduledExecutorService executorService,
            Backoff backoff,
            OptionAutoRevalidate optionAutoRevalidate,
            Class<? extends Throwable>... retryableExceptions) {
        this.distributedLock =
                new SharedSimpleLock(
                        client, key, executorService, backoff, optionAutoRevalidate, retryableExceptions);
        this.memorySemaphore = new Semaphore(1);
    }

    @Override
    public LockStatus getStatus() {
        return distributedLock.getStatus();
    }

    @Override
    public CompletableFuture<Void> lock() {
        return lock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> tryLock() {
        return tryLock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> unlock() {
        return unlock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> lock(ExecutorService executorService) {
        try {
            memorySemaphore.acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(ex);
        }
        return distributedLock
                .lock(executorService)
                .whenComplete(
                        (r, err) -> {
                            if (err != null) {
                                // lock distributed lock failed
                                // rollback the memory lock
                                memorySemaphore.release();
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> tryLock(ExecutorService executorService) {
        if (!memorySemaphore.tryAcquire()) {
            return CompletableFuture.failedFuture(new LockException.LockBusyException());
        }
        return distributedLock
                .tryLock(executorService)
                .whenComplete(
                        (r, err) -> {
                            if (err != null) {
                                // distributed lock failed, rollback the memory lock
                                memorySemaphore.release();
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> unlock(ExecutorService executorService) {
        return distributedLock
                .unlock()
                .whenComplete(
                        (r, err) -> {
                            if (err == null) {
                                // unlock memory lock only when distributed lock unlocked
                                memorySemaphore.release();
                            }
                        });
    }

    @Override
    public void notifyStateChanged(Notification notification) {
        distributedLock.notifyStateChanged(notification);
    }
}
