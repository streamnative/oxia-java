package io.streamnative.oxia.client.lock;

public interface LockManager {

    AsyncLock getLock(String key);
}
