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

import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.ACQUIRED;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.ACQUIRING;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.INIT;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.RELEASED;
import static io.streamnative.oxia.client.api.AsyncLock.LockStatus.RELEASING;
import static io.streamnative.oxia.client.api.exceptions.LockException.AcquireTimeoutException;
import static io.streamnative.oxia.client.api.exceptions.LockException.IllegalLockStatusException;
import static io.streamnative.oxia.client.api.exceptions.LockException.LockBusyException;
import static io.streamnative.oxia.client.util.CompletableFutures.unwrap;
import static io.streamnative.oxia.client.util.CompletableFutures.wrap;
import static io.streamnative.oxia.client.util.Runs.safeExecute;
import static io.streamnative.oxia.client.util.Runs.safeRun;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.google.common.base.Throwables;
import io.grpc.netty.shaded.io.netty.util.internal.PlatformDependent;
import io.grpc.netty.shaded.io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue;
import io.streamnative.oxia.client.api.AsyncLock;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.OptionAutoRevalidate;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.Version;
import io.streamnative.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.exceptions.LockException;
import io.streamnative.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.streamnative.oxia.client.util.Backoff;
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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ThreadSafe
final class LightWeightLock implements AsyncLock {

    private static final Class<? extends Throwable>[] DEFAULT_RETRYABLE_EXCEPTIONS =
            new Class[] {LockBusyException.class};
    private static final byte[] DEFAULT_VALUE = new byte[0];
    private static final AtomicReferenceFieldUpdater<LightWeightLock, LockStatus> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(LightWeightLock.class, LockStatus.class, "state");

    private final AsyncOxiaClient client;
    private final String key;
    private final Backoff backoff;
    private final Set<String> retryableExceptions = new TreeSet<>();
    private final ScheduledExecutorService taskExecutor;
    private final String clientIdentifier;

    LightWeightLock(
            AsyncOxiaClient client,
            String key,
            ScheduledExecutorService executorService,
            Backoff backoff,
            OptionAutoRevalidate revalidate) {
        this(client, key, executorService, backoff, revalidate, DEFAULT_RETRYABLE_EXCEPTIONS);
    }

    @SafeVarargs
    LightWeightLock(
            AsyncOxiaClient client,
            String key,
            ScheduledExecutorService executorService,
            Backoff backoff,
            OptionAutoRevalidate optionAutoRevalidate,
            Class<? extends Throwable>... retryableExceptions) {
        this.client = client;
        this.clientIdentifier = client.getClientIdentifier();
        this.key = key;
        this.state = INIT;
        this.backoff = backoff;
        this.taskExecutor = executorService;
        for (Class<? extends Throwable> retryableException : retryableExceptions) {
            this.retryableExceptions.add(retryableException.getName());
        }
        if (optionAutoRevalidate.enabled()) {
            taskExecutor.scheduleWithFixedDelay(
                    () -> safeRun(log, () -> notifyStateChanged(null)),
                    optionAutoRevalidate.initDelay(),
                    optionAutoRevalidate.delay(),
                    optionAutoRevalidate.unit());
        }
    }

    private volatile LockStatus state;
    private volatile long versionId;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private volatile Optional<Long> sessionId;

    @Override
    public LockStatus getStatus() {
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
        spinLock(callbackService, taskExecutor, f);
        return f;
    }

    private void spinLock(
            ExecutorService callbackService,
            ScheduledExecutorService taskService,
            CompletableFuture<Void> callback) {
        tryLock(callbackService)
                .whenComplete(
                        (r, err) -> {
                            if (err != null) {
                                final Throwable rc = unwrap(err);
                                if (retryableExceptions.contains(rc.getClass().getName())) {
                                    final long ndm = backoff.nextDelayMillis();
                                    if (log.isDebugEnabled()) {
                                        log.debug(
                                                "Acquiring Lock failed, retrying... after {} million seconds. key={} session={} client_id={}",
                                                ndm,
                                                key,
                                                sessionId,
                                                clientIdentifier);
                                    }
                                    // retry later
                                    taskService.schedule(
                                            () -> spinLock(callbackService, taskService, callback),
                                            ndm,
                                            TimeUnit.MILLISECONDS);
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
        while (true) {
            final LockStatus status = STATE_UPDATER.get(this);
            if (status == INIT) {
                // put the future here to ensure wait status MUST initialize future
                if (!STATE_UPDATER.compareAndSet(this, INIT, ACQUIRING)) {
                    continue;
                }
                return tryLock1(Version.KeyNotExists)
                        // don't forget switch thread context
                        .thenAcceptAsync(__ -> {}, callbackService);
            } else if (status == ACQUIRED) {
                return runAsync(
                        () -> {
                            // switch to callback thread here
                            throw wrap(new IllegalLockStatusException(INIT, ACQUIRED));
                        },
                        callbackService);
            } else if (status == ACQUIRING) {
                return runAsync(
                        () -> {
                            // switch to callback thread here
                            throw wrap(new IllegalLockStatusException(INIT, ACQUIRING));
                        },
                        callbackService);
            } else if (status == RELEASING) {
                return runAsync(
                        () -> {
                            // switch to callback thread here
                            throw wrap(new IllegalLockStatusException(INIT, RELEASING));
                        },
                        callbackService);
            } else if (status == RELEASED) {
                STATE_UPDATER.set(this, INIT);
            } else {
                return runAsync(
                        () -> {
                            // switch to callback thread here
                            throw wrap(new LockException.UnknownLockStatusException(status));
                        },
                        callbackService);
            }
        }
    }

    private CompletableFuture<Void> tryLock1(long version) {
        final PutOption versionOption =
                version == Version.KeyNotExists
                        ? PutOption.IfRecordDoesNotExist
                        : PutOption.IfVersionIdEquals(versionId);
        return client
                .put(key, DEFAULT_VALUE, Set.of(PutOption.AsEphemeralRecord, versionOption))
                .thenAccept(
                        result -> {
                            LightWeightLock.this.versionId = result.version().versionId();
                            LightWeightLock.this.sessionId = result.version().sessionId();
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Acquired Lock. key={} session={} client_id={}",
                                        key,
                                        sessionId,
                                        clientIdentifier);
                            }
                            STATE_UPDATER.set(this, ACQUIRED);
                        })
                .exceptionally(
                        ex -> {
                            final Throwable rc = unwrap(ex);
                            final LockException lockE;
                            if (rc instanceof UnexpectedVersionIdException
                                    || rc instanceof KeyAlreadyExistsException) {
                                lockE = new LockBusyException();
                            } else {
                                lockE = LockException.wrap(ex);
                            }
                            // ensure status rollback after exceptional future complete
                            STATE_UPDATER.set(this, RELEASED);
                            throw wrap(lockE);
                        });
    }

    @Override
    public CompletableFuture<Void> tryLock(
            long time, TimeUnit unit, ExecutorService callbackService) {
        return lock(callbackService)
                .orTimeout(time, unit)
                .exceptionally(
                        ex -> {
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
            switch (STATE_UPDATER.get(this)) {
                case INIT -> {
                    return runAsync(
                            () -> {
                                throw wrap(new IllegalLockStatusException(ACQUIRED, INIT));
                            },
                            executorService);
                }
                case ACQUIRING -> {
                    if (log.isDebugEnabled()) {
                        log.debug("busy wait for acquiring. it should be happened very rare.");
                    }
                    try {
                        //noinspection BusyWait
                        Thread.sleep(1);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return runAsync(
                                () -> {
                                    throw wrap(LockException.wrap(ex));
                                });
                    }
                }
                case ACQUIRED -> {
                    if (!STATE_UPDATER.compareAndSet(this, ACQUIRED, RELEASING)) {
                        if (log.isDebugEnabled()) {
                            log.debug("busy wait. expect: acquired, actual: {}", STATE_UPDATER.get(this));
                        }
                        try {
                            //noinspection BusyWait
                            Thread.sleep(1);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            return runAsync(
                                    () -> {
                                        throw wrap(LockException.wrap(ex));
                                    });
                        }
                        continue;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Releasing Lock by unlock." + " key={} session={} client_id={} step={}",
                                key,
                                sessionId,
                                clientIdentifier,
                                ACQUIRED);
                    }
                    return unlock0(executorService);
                }
                case RELEASING, RELEASED -> {
                    return completedFuture(null);
                }
                default -> {
                    return runAsync(
                            () -> {
                                throw wrap(new LockException.UnknownLockStatusException(state));
                            });
                }
            }
        }
    }

    private CompletableFuture<Void> unlock0(ExecutorService executorService) {
        return client
                .delete(key, Set.of(DeleteOption.IfVersionIdEquals(versionId)))
                .thenAcceptAsync(
                        result -> {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Released Lock by unlock. key={} session={} client_id={}",
                                        key,
                                        sessionId,
                                        clientIdentifier);
                            }
                            LightWeightLock.this.versionId = Version.KeyNotExists;
                            LightWeightLock.this.sessionId = Optional.empty();
                            STATE_UPDATER.set(this, RELEASED);
                        },
                        executorService)
                .exceptionallyAsync(
                        ex -> {
                            final var rc = unwrap(ex);
                            if (rc instanceof UnexpectedVersionIdException) {
                                // (1) the lock has been grant by others
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            "Released Lock by session lost when unlock. key={} session={} client_id={}",
                                            key,
                                            sessionId,
                                            clientIdentifier);
                                }
                                STATE_UPDATER.set(this, RELEASED);
                                return null;
                            }
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "unknown issue. key={} session={} client_id={}",
                                        key,
                                        sessionId,
                                        clientIdentifier,
                                        rc);
                            }
                            // todo: better error handling
                            throw new CompletionException(rc);
                        },
                        executorService);
    }

    private void revalidate() {
        // reset
        final List<Notification> notifications = new ArrayList<>();
        revalidateQueue.drain(notifications::add);
        final long currentVersionId = versionId;
        final boolean validSignal =
                notifications.stream()
                        .anyMatch(
                                notification -> {
                                    if (notification instanceof Notification.KeyCreated no
                                            && no.version() <= currentVersionId) {
                                        return false;
                                    }
                                    return !(notification instanceof Notification.KeyModified no)
                                            || no.version() > currentVersionId;
                                });
        if (!validSignal) {
            STATE_UPDATER.set(this, ACQUIRED);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(
                    "Acquiring Lock by revalidation. key={} session={} client_id={}",
                    key,
                    sessionId,
                    clientIdentifier);
        }
        tryLock1(currentVersionId)
                .thenAccept(
                        __ -> {
                            if (log.isDebugEnabled()) {
                                /* serial revalidation */
                                log.debug(
                                        "Acquired Lock by revalidation. key={} session={} client_id={}",
                                        key,
                                        sessionId,
                                        clientIdentifier);
                            }
                        })
                .exceptionally(
                        ex -> {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Released Lock by revalidation. key={} session={} client_id={}",
                                        key,
                                        sessionId,
                                        clientIdentifier,
                                        Throwables.getRootCause(ex));
                            }
                            return null;
                        });
    }

    @SuppressWarnings("unchecked")
    private final MessagePassingQueue<Notification> revalidateQueue =
            (MessagePassingQueue<Notification>) PlatformDependent.newMpscQueue();

    void notifyStateChanged(Notification notification) {
        switch (STATE_UPDATER.get(this)) {
            case INIT, RELEASING, RELEASED -> {
                // no-op
            }
            case ACQUIRING -> {
                if (notification == null) {
                    return;
                }
                revalidateQueue.offer(notification);
            }
            case ACQUIRED -> {
                revalidateQueue.offer(
                        Objects.requireNonNullElseGet(
                                notification,
                                // mock a notification here to trigger the revalidation
                                () -> new Notification.KeyDeleted(key)));
                if (!STATE_UPDATER.compareAndSet(this, ACQUIRED, ACQUIRING)) {
                    return;
                }
                safeExecute(log, taskExecutor, this::revalidate);
            }
        }
    }
}
