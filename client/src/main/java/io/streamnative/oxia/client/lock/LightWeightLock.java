package io.streamnative.oxia.client.lock;

import com.google.common.base.Throwables;
import io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent;
import io.grpc.netty.shaded.io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.streamnative.oxia.client.api.*;
import io.streamnative.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.streamnative.oxia.client.api.exceptions.LockException;
import io.streamnative.oxia.client.util.Backoff;
import lombok.extern.slf4j.Slf4j;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.ACQUIRING;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.INIT;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.RELEASED;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.RELEASING;
import static io.streamnative.oxia.client.api.exceptions.LockException.AcquireTimeoutException;
import static io.streamnative.oxia.client.api.exceptions.LockException.IllegalLockStatusException;
import static io.streamnative.oxia.client.api.exceptions.LockException.LockBusyException;
import static io.streamnative.oxia.client.util.CompletableFutures.unwrap;
import static java.util.concurrent.CompletableFuture.*;

@Slf4j
@ThreadSafe
final class LightWeightLock implements AsyncLock {

    private static final Class<? extends Throwable>[] DEFAULT_RETRYABLE_EXCEPTIONS = new Class[]{LockBusyException.class};
    private static final byte[] DEFAULT_VALUE = new byte[0];
    private static final AtomicReferenceFieldUpdater<LightWeightLock, LockStatus> STATE_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(LightWeightLock.class, LockStatus.class, "state");

    private final AsyncOxiaClient client;
    private final String key;
    private final Backoff backoff;
    private final Set<String> retryableExceptions = new TreeSet<>();
    private final ScheduledExecutorService taskService;
    private final String clientIdentifier;

    LightWeightLock(AsyncOxiaClient client, String key, ScheduledExecutorService executorService, Backoff backoff) {
        this(client, key, executorService, backoff, DEFAULT_RETRYABLE_EXCEPTIONS);
    }

    @SafeVarargs
    LightWeightLock(AsyncOxiaClient client, String key, ScheduledExecutorService executorService,
                    Backoff backoff, Class<? extends Throwable>... retryableExceptions) {
        this.client = client;
        this.clientIdentifier = client.getClientIdentifier();
        this.key = key;
        this.state = INIT;
        this.acquiredF = new CompletableFuture<>();
        this.backoff = backoff;
        this.taskService = executorService;
        for (Class<? extends Throwable> retryableException : retryableExceptions) {
            this.retryableExceptions.add(retryableException.getName());
        }
        registerRevalidateHook();
    }


    private volatile LockStatus state;
    private volatile CompletableFuture<Void> acquiredF;
    private volatile long versionId;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private volatile Optional<Long> sessionId;
    private final AtomicLong operationCounter = new AtomicLong();


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
                    final long ndm = backoff.nextDelayMillis();
                    if (log.isDebugEnabled()) {
                        log.debug("Acquiring Lock failed, retrying... after {} million seconds. key={} session={} client_id={}",
                                ndm, key, sessionId, clientIdentifier);
                    }
                    // retry later
                    taskService.schedule(() -> spinLock(callbackService, taskService, callback),
                            ndm, TimeUnit.MILLISECONDS);
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
        return tryLock0(callbackService);
    }


    private CompletableFuture<Void> tryLock0(ExecutorService callbackService) {
        while (true) {
            final LockStatus status = STATE_UPDATER.get(this);
            if (Objects.requireNonNull(status) == INIT) {
                // put the future here to ensure wait status MUST initialize future
                acquiredF = new CompletableFuture<>();
                if (!STATE_UPDATER.compareAndSet(this, status, ACQUIRING)) {
                    continue;
                }
                tryLock1(Version.KeyNotExists);
                return acquiredF;
            } else if (status == ACQUIRED) {
                return supplyAsync(() -> {
                    // switch to callback thread here
                    throw new CompletionException(new IllegalLockStatusException(INIT, ACQUIRED));
                }, callbackService);
            } else if (status == ACQUIRING) {
                return supplyAsync(() -> {
                    // switch to callback thread here
                    throw new CompletionException(new IllegalLockStatusException(INIT, ACQUIRING));
                }, callbackService);
            } else if (status == RELEASING) {
                return supplyAsync(() -> {
                    // switch to callback thread here
                    throw new CompletionException(new IllegalLockStatusException(INIT, RELEASING));
                }, callbackService);
            } else if (status == RELEASED) {
                STATE_UPDATER.set(this, INIT);
            } else {
                return supplyAsync(() -> {
                    // switch to callback thread here
                    throw new CompletionException(new LockException.UnkonwnLockStatusException(status));
                }, callbackService);
            }
        }
    }

    private void tryLock1(long version) {
        final PutOption versionOption = version == Version.KeyNotExists ?
                PutOption.IfRecordDoesNotExist : PutOption.IfVersionIdEquals(versionId);
        client.put(key, DEFAULT_VALUE, Set.of(PutOption.AsEphemeralRecord, versionOption))
                .thenAcceptAsync(result -> {
                    LightWeightLock.this.versionId = result.version().versionId();
                    LightWeightLock.this.sessionId = result.version().sessionId();
                    log.info("Acquired Lock. key={} session={} client_id={}",
                            key, sessionId, clientIdentifier);
                    acquiredF.complete(null);
                    if (!STATE_UPDATER.compareAndSet(this, ACQUIRING, ACQUIRED)) {
                        log.info("unexpected status when checking status. expect {} actual {}",
                                ACQUIRING, STATE_UPDATER.get(this));
                    }
                }).exceptionallyAsync(ex -> {
                    final Throwable rc = unwrap(ex);
                    final LockException lockE;
                    if (rc instanceof UnexpectedVersionIdException
                        || rc instanceof KeyAlreadyExistsException) {
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
                    STATE_UPDATER.set(this, RELEASED);
                    return null;
                });
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
        while (true) {
            switch (state) {
                case INIT -> {
                    return failedFuture(new IllegalLockStatusException(ACQUIRED, INIT));
                }
                case ACQUIRING -> {
                    final CompletableFuture<Void> f = new CompletableFuture<>();
                    acquiredF.whenComplete((r, err) -> {
                        STATE_UPDATER.set(this, RELEASING);
                        spinUnlock(executorService).whenComplete((r2, err2) -> {
                            if (err2 != null) {
                                f.completeExceptionally(err2);
                            } else {
                                f.complete(null);
                            }
                        });
                    });
                    return f;
                }
                case ACQUIRED -> {
                    if (!STATE_UPDATER.compareAndSet(this, ACQUIRED, RELEASING)) {
                        Thread.onSpinWait();
                        continue;
                    }
                    return spinUnlock(executorService);
                }
                case RELEASING, RELEASED -> {
                    return completedFuture(null);
                }
                default -> {
                    return failedFuture(new LockException.UnkonwnLockStatusException(state));
                }
            }
        }
    }

    private CompletableFuture<Void> spinUnlock(ExecutorService executorService) {
        final long stamp = operationCounter.incrementAndGet();
        return client.delete(key, Set.of(DeleteOption.IfVersionIdEquals(versionId)))
                .thenAcceptAsync(result -> {
                    log.info("Released Lock by unlock. key={} session={} client_id={}", key, sessionId, clientIdentifier);
                    LightWeightLock.this.versionId = Version.KeyNotExists;
                    LightWeightLock.this.sessionId = Optional.empty();
                    STATE_UPDATER.set(this, RELEASED);
                }, executorService)
                .exceptionallyAsync(ex -> {
                    final var rc = unwrap(ex);
                    if (rc instanceof UnexpectedVersionIdException) {
                        // (1) the lock has been grant by others
                        if (stamp == operationCounter.get()) {
                            STATE_UPDATER.set(this, RELEASED);
                            log.info("Released Lock by session lost when unlock. key={} session={} client_id={}",
                                    key, sessionId, clientIdentifier);
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
            // reset
            revalidateTriggerFuture = new CompletableFuture<>();
            final List<Notification> notifications = new ArrayList<>();
            revalidateQueue.drain(notifications::add);
            final long currentVersionId = versionId;
            final List<Notification> ns = notifications.stream().filter(notification -> {
                if (notification instanceof Notification.KeyCreated no && no.version() <= currentVersionId) {
                    return false;
                }
                return !(notification instanceof Notification.KeyModified no) || no.version() > currentVersionId;
            }).toList();
            if (ns.isEmpty()) {
                acquiredF.complete(null);
                return;
            }

            final long stamp = operationCounter.incrementAndGet();
            tryLock1(currentVersionId);
            acquiredF.thenAcceptAsync(__ -> {
                        /* serial revalidation */
                        log.info("Acquired Lock by revalidation. key={} session={} client_id={}",
                                key, sessionId, clientIdentifier);
                        registerRevalidateHook();
                    }, taskService)
                    .exceptionally(ex -> {
                        var rc = unwrap(ex);
                        if (rc instanceof LockBusyException) {
                            // (1) the lock is released by unlock,
                            if (stamp != operationCounter.get()) {
                                spinRevalidate();
                                return null;
                            }
                            // (2) the lock has been grant by others
                        }
                        log.info("Released Lock by revalidation. key={} session={} client_id={}",
                                key, sessionId, clientIdentifier, Throwables.getRootCause(rc));
                        STATE_UPDATER.set(this, RELEASED);
                        return null;
                    });
        });
    }

    @SuppressWarnings("unchecked")
    private final MessagePassingQueue<Notification> revalidateQueue =
            (MessagePassingQueue<Notification>) PlatformDependent.newMpscQueue();
    private volatile CompletableFuture<Void> revalidateTriggerFuture = new CompletableFuture<>();

    void notifyStateChanged(Notification notification) {
        switch (STATE_UPDATER.get(this)) {
            case INIT, RELEASING, RELEASED -> {
                // no-op
            }
            case ACQUIRING -> {
                /*
                  In this case, the server might just get the lock and receive the notification.
                  Therefore, we have to revalidate the lock again.
                 */
                // status MUST ensure the acquireF can not be null
                acquiredF.whenComplete((r, err) -> {
                    if (err == null) {
                        // reset flag
                        // status MUST ensure the acquireF callback after status changes
                        notifyStateChanged(notification);
                    }
                    // don't need worry about failed acquire
                });
            }
            case ACQUIRED -> {
                /* The lock has been released */
                revalidateQueue.offer(notification);
                acquiredF = new CompletableFuture<>();
                if (!STATE_UPDATER.compareAndSet(this, ACQUIRED, ACQUIRING)) {
                    return;
                }
                revalidateTriggerFuture.complete(null);
            }
        }
    }
}
