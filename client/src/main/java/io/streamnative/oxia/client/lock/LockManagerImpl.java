package io.streamnative.oxia.client.lock;

import io.streamnative.oxia.client.api.*;
import io.streamnative.oxia.client.util.Backoff;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

final class LockManagerImpl implements LockManager, Consumer<Notification> {
    private final AsyncOxiaClient client;
    private final Map<String, LightWeightLock> locks;
    private final ScheduledExecutorService executor;

    LockManagerImpl(AsyncOxiaClient client, ScheduledExecutorService scheduledExecutorService) {
        this.client = client;
        this.locks = new ConcurrentHashMap<>();
        this.executor = scheduledExecutorService;
        // register self as the notification receiver
        client.notifications(this);
    }

    @Override
    public AsyncLock getLock(String key, OptionBackoff optionBackoff) {
        return locks.computeIfAbsent(key, (k) -> new LightWeightLock(client, key, executor,
                new Backoff(optionBackoff.initDelay(), optionBackoff.initDelayUnit(),
                        optionBackoff.maxDelay(), optionBackoff.maxDelayUnit(), optionBackoff.clock())));
    }

    @Override
    public void accept(Notification notification) {
        final var lock = locks.get(notification.key());
        if (lock == null) {
            return;
        }
        if (notification instanceof Notification.KeyDeleted) {
            lock.notifyStateChanged();
        }
    }
}
