package io.streamnative.oxia.client.grpc;

import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PACKAGE;

import com.google.common.annotations.VisibleForTesting;
import java.util.Random;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
@VisibleForTesting
public class ExponentialBackoff implements LongFunction<Long> {
    private final Supplier<Long> randomLongSupplier;
    private final long maxRandom;
    private final long maxIntervalMs;

    public ExponentialBackoff() {
        var random = new Random();
        randomLongSupplier = random::nextLong;
        this.maxRandom = 500L;
        this.maxIntervalMs = SECONDS.toMillis(30);
    }

    @Override
    public Long apply(long retryIndex) {
        return Math.min(
                ((long) Math.pow(2.0, retryIndex)) + (Math.abs(randomLongSupplier.get()) % maxRandom),
                maxIntervalMs);
    }
}
