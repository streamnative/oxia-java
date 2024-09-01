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
