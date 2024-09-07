package io.streamnative.oxia.client.grpc;

import io.grpc.internal.BackoffPolicy;
import io.streamnative.oxia.client.util.Backoff;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public final class OxiaBackoffProvider implements BackoffPolicy.Provider {
    public static final BackoffPolicy.Provider DEFAULT = new OxiaBackoffProvider(Backoff.DEFAULT_INITIAL_DELAY_MILLIS,
            TimeUnit.MILLISECONDS, Backoff.DEFAULT_MAX_DELAY_SECONDS,
            TimeUnit.MILLISECONDS);
    private final long initialDelay;
    private final TimeUnit unitInitialDelay;
    private final long maxDelay;
    private final TimeUnit unitMaxDelay;

    OxiaBackoffProvider(long initialDelay,
                        TimeUnit unitInitialDelay,
                        long maxDelay,
                        TimeUnit unitMaxDelay) {
        this.initialDelay = initialDelay;
        this.unitInitialDelay = unitInitialDelay;
        this.maxDelay = maxDelay;
        this.unitMaxDelay = unitMaxDelay;
    }

    @Override
    public BackoffPolicy get() {
        return new Backoff(initialDelay, unitInitialDelay, maxDelay, unitMaxDelay);
    }

    public static BackoffPolicy.Provider create(Duration minDelay, Duration maxDelay) {
        return new OxiaBackoffProvider(minDelay.getNano(),
                TimeUnit.NANOSECONDS, maxDelay.getNano(), TimeUnit.NANOSECONDS);
    }
}
