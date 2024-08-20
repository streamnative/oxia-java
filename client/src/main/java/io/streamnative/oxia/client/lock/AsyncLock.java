package io.streamnative.oxia.client.lock;

import java.util.concurrent.CompletableFuture;
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

    CompletableFuture<Boolean> tryLock();

    CompletableFuture<Boolean> tryLock(long time, TimeUnit unit) throws InterruptedException;

    CompletableFuture<Void> unlock();
}
