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
package io.streamnative.oxia.client.api;

import java.io.Closeable;

public interface LockManager extends Closeable {

    /**
     * Gets a lightweight asynchronous lock for the specified key with default backoff options.
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
     * @param key the key associated with the lock
     * @param optionBackoff the backoff options to be used for lock acquisition retries
     * @return an AsyncLock instance for the specified key
     */
    AsyncLock getLightWeightLock(String key, OptionBackoff optionBackoff);
}
