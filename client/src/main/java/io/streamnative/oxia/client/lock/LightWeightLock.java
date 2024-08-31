package io.streamnative.oxia.client.lock;

import com.google.common.base.Throwables;
import io.streamnative.oxia.client.api.AsyncLock;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.Version;
import io.streamnative.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.streamnative.oxia.client.api.exceptions.LockException;
import io.streamnative.oxia.client.util.Backoff;
import lombok.extern.slf4j.Slf4j;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.ACQUIRED;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.INIT;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.RELEASED;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.WAIT;
import static io.streamnative.oxia.client.api.exceptions.LockException.AcquireTimeoutException;
import static io.streamnative.oxia.client.api.exceptions.LockException.IllegalLockStatusException;
import static io.streamnative.oxia.client.api.exceptions.LockException.LockBusyException;
import static io.streamnative.oxia.client.util.CompletableFutures.unwrap;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

@Slf4j
final class LightWeightLock implements AsyncLock {

    private static final Class<? extends Throwable>[] DEFAULT_RETRYABLE_EXCEPTIONS = new Class[]{LockBusyException.class};
    private static final byte[] DEFAULT_VALUE = new byte[0];
    private static final AtomicReferenceFieldUpdater<LightWeightLock, Status> STATE_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(LightWeightLock.class, Status.class, "state");

    private final AsyncOxiaClient client;
    private final String key;
    private final Backoff backoff;
    private final Set<String> retryableExceptions = new TreeSet<>();
    private final ScheduledExecutorService taskService;

    LightWeightLock(AsyncOxiaClient client, String key, ScheduledExecutorService executorService, Backoff backoff) {
        this(client, key, executorService, backoff, DEFAULT_RETRYABLE_EXCEPTIONS);
    }

    @SafeVarargs
    LightWeightLock(AsyncOxiaClient client, String key, ScheduledExecutorService executorService,
                    Backoff backoff, Class<? extends Throwable>... retryableExceptions) {
        this.client = client;
        this.key = key;
        this.state = new Status(INIT, System.currentTimeMillis());
        this.acquiredF = new CompletableFuture<>();
        this.backoff = backoff;
        this.taskService = executorService;
        for (Class<? extends Throwable> retryableException : retryableExceptions) {
            this.retryableExceptions.add(retryableException.getName());
        }
        registerRevalidateHook();
    }


    private volatile Status state;
    private volatile CompletableFuture<Void> acquiredF;
    private volatile long versionId;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private volatile Optional<Long> sessionId;
    private final AtomicLong operationCounter = new AtomicLong();


    @Override
    public Status getStatus() {
        return STATE_UPDATER.get(this);
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
    public CompletableFuture<Void> tryLock(long time, TimeUnit unit) {
        return tryLock(time, unit, ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> unlock() {
        return unlock(ForkJoinPool.commonPool());
    }

    @Override
    public CompletableFuture<Void> lock(ExecutorService callbackService) {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        spinLock(callbackService, taskService, f);
        return f;
    }

    private void spinLock(ExecutorService callbackService, ScheduledExecutorService taskService, CompletableFuture<Void> callback) {
        tryLock(callbackService).whenComplete((r, err) -> {
            if (err != null) {
                final Throwable rc = unwrap(err);
                if (retryableExceptions.contains(rc.getClass().getName())) {
                    // retry later
                    taskService.schedule(() -> spinLock(callbackService, taskService, callback),
                            backoff.nextDelayMillis(), TimeUnit.MILLISECONDS);
                } else {
                    callback.completeExceptionally(err);
                }
            } else {
                callback.complete(null);
            }
        });
    }

    @Override
    public CompletableFuture<Void> tryLock(ExecutorService callbackService) {
        operationCounter.incrementAndGet();
        return tryLock0(callbackService, Version.KeyNotExists);
    }


    private CompletableFuture<Void> tryLock0(ExecutorService callbackService, long version) {
        final Status status = STATE_UPDATER.get(this);
        while (true) {
            switch (status.lockStatus()) {
                case INIT -> {
                    // put the future here to ensure wait status MUST initialize future
                    acquiredF = new CompletableFuture<>();
                    final Status waitStatus = new Status(WAIT, System.currentTimeMillis());
                    if (!STATE_UPDATER.compareAndSet(this, status, waitStatus)) {
                        continue;
                    }
                    client.put(key, DEFAULT_VALUE, Set.of(PutOption.AsEphemeralRecord,
                                    PutOption.IfVersionIdEquals(version)))
                            .thenAcceptAsync(result -> {
                                LightWeightLock.this.versionId = result.version().versionId();
                                LightWeightLock.this.sessionId = result.version().sessionId();
                                STATE_UPDATER.set(this, new Status(ACQUIRED, System.currentTimeMillis()));
                                acquiredF.complete(null);
                            }).exceptionallyAsync(ex -> {
                                final Throwable rc = unwrap(ex);
                                final LockException lockE;
                                if (rc instanceof UnexpectedVersionIdException) {
                                    lockE = new LockBusyException();
                                } else {
                                    lockE = LockException.wrap(ex);
                                }
                                try {
                                    acquiredF.completeExceptionally(lockE);
                                } catch (Throwable e2) {
                                    log.warn("BUG! we shouldn't throw exception when callback");
                                }
                                // ensure status rollback after exceptional future complete
                                STATE_UPDATER.set(this, new Status(RELEASED, System.currentTimeMillis()));
                                return null;
                            });
                    return acquiredF;
                }
                case WAIT, ACQUIRED -> {
                    return supplyAsync(() -> {
                        // switch to callback thread here
                        throw new CompletionException(new IllegalLockStatusException(INIT, ACQUIRED, "Illegal status"));
                    }, callbackService);
                }
                case RELEASED -> {
                    final Status initStatus = new Status(INIT, System.currentTimeMillis());
                    STATE_UPDATER.compareAndSet(this, status, initStatus);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> tryLock(long time, TimeUnit unit, ExecutorService callbackService) {
        return lock(callbackService).orTimeout(time, unit)
                .exceptionally(ex -> {
                    final Throwable rc = unwrap(ex);
                    if (rc instanceof CancellationException) {
                        throw new CompletionException(new AcquireTimeoutException());
                    }
                    throw new CompletionException(LockException.wrap(rc));
                });
    }

    @Override
    public CompletableFuture<Void> unlock(ExecutorService executorService) {
        switch (state.lockStatus()) {
            case INIT -> {
                return failedFuture(new IllegalLockStatusException(ACQUIRED, INIT, "unexpected lock status"));
            }
            case WAIT -> {
                return failedFuture(new IllegalLockStatusException(ACQUIRED, WAIT, "unexpected lock status"));
            }
            case ACQUIRED -> {
                return spinUnlock(executorService);
            }
            case RELEASED -> {
                return failedFuture(new IllegalLockStatusException(ACQUIRED,
                        RELEASED, "unexpected lock status"));
            }
            default -> {
                return failedFuture(new IllegalLockStatusException(ACQUIRED, null,
                        "unexpected lock status"));
            }
        }
    }

    private CompletableFuture<Void> spinUnlock(ExecutorService executorService) {
        final long stamp = operationCounter.incrementAndGet();
        return client.delete(key, Set.of(DeleteOption.IfVersionIdEquals(versionId)))
                .thenAcceptAsync(result -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Lock {} is released by unlock", key);
                    }
                    LightWeightLock.this.versionId = Version.KeyNotExists;
                    LightWeightLock.this.sessionId = Optional.empty();
                    STATE_UPDATER.set(this, new Status(RELEASED, System.currentTimeMillis()));
                }, executorService)
                .exceptionallyAsync(ex -> {
                    final var rc = unwrap(ex);
                    if (rc instanceof UnexpectedVersionIdException) {
                        // (1) the lock has been grant by others
                        if (stamp == operationCounter.get()) {
                            STATE_UPDATER.set(this, new Status(RELEASED, System.currentTimeMillis()));
                            return null;
                        }
                        // (2) the lock is revalidating by notification
                        spinUnlock(executorService);
                    }
                    throw new CompletionException(rc);
                }, executorService);
    }

    private void registerRevalidateHook() {
        revalidateTriggerFuture.whenComplete((r, err) -> {
            spinRevalidate();
        });
    }

    private void spinRevalidate() {
        taskService.execute(() -> {
            // reset trigger
            revalidateTriggerFuture = new CompletableFuture<>();
            final long stamp = operationCounter.incrementAndGet();
            tryLock0(ForkJoinPool.commonPool(), versionId)
                    .thenAcceptAsync(__ -> {
                        /* serial revalidation */
                        registerRevalidateHook();
                    }, taskService)
                    .exceptionally(ex -> {
                        var rc = unwrap(ex);
                        if (rc instanceof LockBusyException) {
                            // (2) the lock is released by unlock,
                            if (stamp != operationCounter.get()) {
                                spinRevalidate();
                                return null;
                            }
                            // (1) the lock has been grant by others
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Lock {} is released by session timeout. revalidate error: ",
                                    key, Throwables.getRootCause(rc));
                        }
                        STATE_UPDATER.set(this, new Status(RELEASED, System.currentTimeMillis()));
                        return null;
                    });
        });
    }

    private volatile CompletableFuture<Void> revalidateTriggerFuture = new CompletableFuture<>();

    void notifyStateChanged() {
        switch (state.lockStatus()) {
            case INIT, RELEASED -> {
                // no-op
            }
            case WAIT -> {
                /*
                  In this case, the server might just get the lock and receive the notification.
                  Therefore, we have to revalidate the lock again.
                 */
                // status MUST ensure the acquireF can not be null
                acquiredF.whenComplete((r, err) -> {
                    if (err == null) {
                        // status MUST ensure the acquireF callback after status changes
                        // same as normal acquire revalidation
                        notifyStateChanged();
                    }
                    // don't need worry about failed acquire
                });
            }
            case ACQUIRED -> {
                /* The lock has been released */
                revalidateTriggerFuture.complete(null);
            }
        }
    }
}
