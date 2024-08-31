package io.streamnative.oxia.client.lock;

import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.LockManager;
import lombok.experimental.UtilityClass;
import java.util.Objects;
import java.util.concurrent.Executors;

@UtilityClass
public final class LockManagers {

    public static LockManager createLockManager(AsyncOxiaClient client) {
        Objects.requireNonNull(client);
        return new LockManagerImpl(client,
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("oxia-lock-manager")));
    }
}
