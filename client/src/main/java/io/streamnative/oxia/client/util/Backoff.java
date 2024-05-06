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
package io.streamnative.oxia.client.util;

import java.time.Clock;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Backoff {
    private final long initialDelayMillis;
    private long nextDelayMillis;
    private final long maxDelayMillis;
    private final Clock clock;

    private static final long DEFAULT_INITIAL_DELAY_MILLIS = 100;
    private static final long DEFAULT_MAX_DELAY_SECONDS = 60;

    public Backoff() {
        this(
                DEFAULT_INITIAL_DELAY_MILLIS,
                TimeUnit.MILLISECONDS,
                DEFAULT_MAX_DELAY_SECONDS,
                TimeUnit.SECONDS,
                Clock.systemUTC());
    }

    public Backoff(
            long initialDelay,
            TimeUnit unitInitialDelay,
            long maxDelay,
            TimeUnit unitMaxDelay,
            Clock clock) {
        this.initialDelayMillis = unitInitialDelay.toMillis(initialDelay);
        this.maxDelayMillis = unitMaxDelay.toMillis(maxDelay);
        this.clock = clock;
        this.nextDelayMillis = initialDelayMillis;
    }

    public long nextDelayMillis() {
        long currentDelayMillis = this.nextDelayMillis;
        if (currentDelayMillis < maxDelayMillis) {
            this.nextDelayMillis = Math.min(this.nextDelayMillis * 2, this.maxDelayMillis);
        }

        // Randomize with +- 10%
        return ThreadLocalRandom.current()
                .nextLong(currentDelayMillis, (long) (currentDelayMillis * 1.2));
    }

    public void reset() {
        this.nextDelayMillis = initialDelayMillis;
    }
}
