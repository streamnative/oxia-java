package io.streamnative.oxia.client.lock;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.Version;
import io.streamnative.oxia.client.api.exceptions.UnexpectedVersionIdException;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.streamnative.oxia.client.lock.AsyncLock.LockStatus.*;
import static java.util.concurrent.CompletableFuture.*;

@Slf4j
final class ArcDsLock implements AsyncLock {

    ArcDsLock(AsyncOxiaClient client, String key) {
        this.client = client;
        this.key = key;
        this.state = new Status(INIT, System.currentTimeMillis());
        this.acquiredF = new CompletableFuture<>();
    }

    private static final byte[] DEFAULT_VALUE = new byte[0];
    private volatile Status state;
    private final AsyncOxiaClient client;
    private final String key;
    private static final AtomicReferenceFieldUpdater<ArcDsLock, Status> STATE_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(ArcDsLock.class, Status.class, "state");
    private final AtomicInteger ref = new AtomicInteger(0);
    private volatile CompletableFuture<Boolean> acquiredF;
    private volatile Version version;

    @Override
    public Status getStatus() {
        return STATE_UPDATER.get(this);
    }

    @Override
    public CompletableFuture<Void> lock() {
        return tryLock().thenAccept(version -> {

        });
    }

    @Override
    public CompletableFuture<Boolean> tryLock() {
        final Status status = STATE_UPDATER.get(this);
        while (true) {
            switch (status.lockStatus()) {
                case INIT -> {
                    final Status waitStatus = new Status(WAIT, System.currentTimeMillis());
                    if (!STATE_UPDATER.compareAndSet(this, state, waitStatus)) {
                        continue;
                    }
                    // do acquire logic
                    acquiredF = client.put(key, DEFAULT_VALUE, Set.of(PutOption.AsEphemeralRecord,
                                    PutOption.IfRecordDoesNotExist))
                            .thenApplyAsync(result -> {
                                ArcDsLock.this.version = result.version();
                                STATE_UPDATER.set(this, new Status(ACQUIRED, System.currentTimeMillis()));
                                return true;
                            }).exceptionallyAsync(ex -> {
                                final Throwable rc = ex.getCause();
                                if (rc instanceof UnexpectedVersionIdException) {
                                    return false;
                                }
                                log.warn("unexpected exception when acquiring lock. key={}", key, rc);
                                return false;
                            });
                    return acquiredF;
                }
                case WAIT -> {
                    ref.incrementAndGet();
                    return acquiredF;
                }
                case ACQUIRED -> {
                    ref.incrementAndGet();
                    return completedFuture(true);
                }
                case RELEASED -> {
                    return failedFuture(new LockException
                            .IllegalLockStatusException(INIT, RELEASED, "unexpected lock status"));
                }
            }
        }
    }

    @Override
    public CompletableFuture<Boolean> tryLock(long time, TimeUnit unit) {
        return lock().orTimeout(time, unit)
                .thenApply(__ -> true)
                .exceptionally(ex -> {
                    final Throwable rc = ex.getCause();
                    if (rc instanceof CancellationException) {
                        return false;
                    }
                    log.warn("unexpected exception when try acquiring lock. key={}", key, rc);
                    return false;
                });
    }

    @Override
    public CompletableFuture<Void> unlock() {
        switch (state.lockStatus()) {
            case INIT -> {
                return failedFuture(new LockException.IllegalLockStatusException(ACQUIRED, INIT, "unexpected lock status"));
            }
            case WAIT -> {
                return failedFuture(new LockException.IllegalLockStatusException(ACQUIRED, WAIT, "unexpected lock status"));
            }
            case ACQUIRED -> {
                if (ref.incrementAndGet() != 0) {
                    return completedFuture(null);
                }
                return client.delete(key, Set.of(DeleteOption.IfVersionIdEquals(version.versionId())))
                        .thenAccept(result -> {
                            STATE_UPDATER.set(this, new Status(RELEASED, System.currentTimeMillis()));
                        })
                        .exceptionallyAsync(ex -> {
                            final Throwable rc = ex.getCause();
                            if (rc instanceof UnexpectedVersionIdException) {
                                // the lock has been grant by others
                                STATE_UPDATER.set(this, new Status(RELEASED, System.currentTimeMillis()));
                                return null;
                            }
                            log.warn("unexpected exception when acquiring lock. key={}", key, rc);
                            return null;
                        });
            }
            case RELEASED -> {
                return failedFuture(new LockException.IllegalLockStatusException(ACQUIRED,
                        RELEASED, "unexpected lock status"));
            }
            default -> {
                return failedFuture(new LockException.IllegalLockStatusException(ACQUIRED, null,
                        "unexpected lock status"));
            }
        }
    }

    void notifyStateChanged() {
        switch (state.lockStatus()) {
            case INIT, WAIT, RELEASED -> {
                // no-op
            }
            case ACQUIRED -> {
                /*
                    The lock has been released and
                 */
                tryLock().thenAccept(acquired -> {
                    if (acquired) {

                    } else {

                    }
                }).exceptionally(ex -> {

                    return null;
                });
            }
        }
    }
}
