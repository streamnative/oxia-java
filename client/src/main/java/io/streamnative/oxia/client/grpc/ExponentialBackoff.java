/*
 * Copyright © 2022-2023 StreamNative Inc.
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
