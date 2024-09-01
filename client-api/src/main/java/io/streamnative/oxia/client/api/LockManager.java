package io.streamnative.oxia.client.api;

public interface LockManager {

    /**
     * Gets a lightweight asynchronous lock for the specified key with default backoff options.
     *
     *
     * @param key the key associated with the lock
     * @return an AsyncLock instance for the specified key
     */
    default AsyncLock getLightWeightLock(String key) {
        return getLightWeightLock(key, OptionBackoff.DEFAULT);
    }


    /**
     * Gets a lightweight asynchronous lock for the specified key with custom backoff options.
     *
     *
     * @param key           the key associated with the lock
     * @param optionBackoff the backoff options to be used for lock acquisition retries
     * @return an AsyncLock instance for the specified key
     */
    AsyncLock getLightWeightLock(String key, OptionBackoff optionBackoff);
}
