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

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.LockManager;
import io.streamnative.oxia.client.api.OptionAutoRevalidate;
import lombok.experimental.UtilityClass;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


@UtilityClass
public final class LockManagers {

    /**
     * Creates a LockManager with a default single-thread ScheduledExecutorService and default OptionAutoRevalidate.
     *
     * @param client the AsyncOxiaClient to be used by the LockManager
     * @return a new LockManager instance
     */
    public static LockManager createLockManager(AsyncOxiaClient client) {
        Objects.requireNonNull(client);
        return new LockManagerImpl(client,
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("oxia-lock-manager")),
                OptionAutoRevalidate.DEFAULT);
    }

    /**
     * Creates a LockManager with a custom ScheduledExecutorService and OptionAutoRevalidate.
     *
     * @param client               the AsyncOxiaClient to be used by the LockManager
     * @param service              the ScheduledExecutorService to be used
     * @param optionAutoRevalidate the OptionAutoRevalidate setting to be used
     * @return a new LockManager instance
     */
    public static LockManager createLockManager(AsyncOxiaClient client,
                                                ScheduledExecutorService service,
                                                OptionAutoRevalidate optionAutoRevalidate) {
        Objects.requireNonNull(client);
        return new LockManagerImpl(client, service, optionAutoRevalidate);
    }
}
