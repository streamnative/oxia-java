package io.streamnative.oxia.client.api;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

public record OptionBackoff(long initDelay, TimeUnit initDelayUnit,
                            long maxDelay, TimeUnit maxDelayUnit,
                            Clock clock) {
    public static OptionBackoff DEFAULT = new OptionBackoff(10, MILLISECONDS, 200, MILLISECONDS, Clock.systemUTC());
}
