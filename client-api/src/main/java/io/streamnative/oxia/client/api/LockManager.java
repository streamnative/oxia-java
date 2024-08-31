package io.streamnative.oxia.client.api;

public interface LockManager {

    default AsyncLock getLock(String key) {
        return getLock(key, OptionBackoff.DEFAULT);
    }

    AsyncLock getLock(String key, OptionBackoff optionBackoff);
}
