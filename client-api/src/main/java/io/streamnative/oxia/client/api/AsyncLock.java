package io.streamnative.oxia.client.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public interface AsyncLock {

    enum LockStatus {
        INIT,
        WAIT,
        ACQUIRED,
        RELEASED;
    }

    record Status(LockStatus lockStatus, long timestamp) {
    }

    Status getStatus();

    CompletableFuture<Void> lock();

    CompletableFuture<Void> tryLock();

    CompletableFuture<Void> tryLock(long time, TimeUnit unit);

    CompletableFuture<Void> unlock();

    CompletableFuture<Void> lock(ExecutorService executorService);

    CompletableFuture<Void> tryLock(ExecutorService executorService);

    CompletableFuture<Void> tryLock(long time, TimeUnit unit, ExecutorService executorService);

    CompletableFuture<Void> unlock(ExecutorService executorService);
}
