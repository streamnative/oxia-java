package io.streamnative.oxia.client.lock;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.Notification;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

final class LockManagerImpl implements LockManager, Consumer<Notification> {
    private final AsyncOxiaClient client;
    private final Map<String, ArcDsLock> locks;

    LockManagerImpl(AsyncOxiaClient client) {
        this.client = client;
        this.locks = new ConcurrentHashMap<>();
        // register self as the notification receiver
        client.notifications(this);
    }

    @Override
    public AsyncLock getLock(String key) {
        return locks.compute(key, (k, v) -> {
            if (v == null || v.getStatus().lockStatus() == AsyncLock.LockStatus.RELEASED) {
                return new ArcDsLock(client, key);
            }
            return v;
        });
    }

    @Override
    public void accept(Notification notification) {
        final var lock = locks.get(notification.key());
        if (lock == null) {
            return;
        }
        if (notification instanceof Notification.KeyDeleted) {
            // someone's session is closed, notify lock to re-try
            if (lock.getStatus().lockStatus() == AsyncLock.LockStatus.RELEASED) {
                locks.remove(notification.key());
            }
            lock.notifyStateChanged();
        }
    }
}
