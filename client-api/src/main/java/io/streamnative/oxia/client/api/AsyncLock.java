package io.streamnative.oxia.client.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public interface AsyncLock {

    /**
     * Represents the different status of a lock.
     */
    enum LockStatus {
        INIT,
        ACQUIRING,
        ACQUIRED,
        RELEASING,
        RELEASED;
    }

    /**
     * Asynchronously acquires the lock.
     *
     * @return a CompletableFuture that completes when the lock is acquired
     */
    CompletableFuture<Void> lock();

    /**
     * Tries to acquire the lock asynchronously.
     *
     * @return a CompletableFuture that completes when the lock is acquired or not
     */
    CompletableFuture<Void> tryLock();

    /**
     * Tries to acquire the lock asynchronously within a specified time.
     *
     * @param time the time to wait
     * @param unit the time unit of the time parameter
     * @return a CompletableFuture that completes when the lock is acquired or not within the specified time
     */
    CompletableFuture<Void> tryLock(long time, TimeUnit unit);

    /**
     * Asynchronously releases the lock.
     *
     * @return a CompletableFuture that completes when the lock is released
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
     * @return a CompletableFuture that completes when the lock is acquired or not
     */
    CompletableFuture<Void> tryLock(ExecutorService executorService);

    /**
     * Tries to acquire the lock asynchronously within a specified time using a specified ExecutorService.
     *
     * @param time the time to wait
     * @param unit the time unit of the time parameter
     * @param executorService the ExecutorService to use for acquiring the lock
     * @return a CompletableFuture that completes when the lock is acquired or not within the specified time
     */
    CompletableFuture<Void> tryLock(long time, TimeUnit unit, ExecutorService executorService);

    /**
     * Asynchronously releases the lock using a specified ExecutorService.
     *
     * @param executorService the ExecutorService to use for releasing the lock
     * @return a CompletableFuture that completes when the lock is released
     */
    CompletableFuture<Void> unlock(ExecutorService executorService);
}
