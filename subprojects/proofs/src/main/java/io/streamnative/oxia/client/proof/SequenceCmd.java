/*
 * Copyright © 2022-2025 StreamNative Inc.
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
package io.streamnative.oxia.client.proof;

import com.google.common.util.concurrent.RateLimiter;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.PutOption;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@CommandLine.Command(name = "sequence")
public final class SequenceCmd extends BaseCmd implements Runnable, Exec {

    private AsyncOxiaClient client;

    @Override
    public void run() {
        this.client = OxiaClientBuilder.create(serviceAddr)
                .namespace(namespace)
                .asyncClient()
                .join();
        exec();
    }

    @Override
    public void exec() {
        log.info("starting proof test...");
        final RateLimiter rateLimiter = RateLimiter.create(requestsRate);

        for (int i = 0; i < keyNum; i++) {
            final String key = UUID.randomUUID().toString();
            log.info("starting delta proof test.  key={}", key);
            Thread.ofVirtual()
                    .name("proof-" + key)
                    .start(() -> {
                        final AtomicLong payloadCounter = new AtomicLong(0);
                        final AtomicLong expectDeltaL = new AtomicLong(1);
                        final AtomicLong expectDeltaM = new AtomicLong(2);
                        final AtomicLong expectDeltaR = new AtomicLong(3);
                        log.info("delta proof test is ready. key={}", key);
                        while (true) {
                            rateLimiter.acquire();
                            final ByteBuffer buf = ByteBuffer.allocate(8);
                            buf.putLong(payloadCounter.getAndIncrement());
                            final var sendFuture = client.put(key, buf.array(), Set.of(PutOption.SequenceKeysDeltas(List.of(1L, 2L, 3L)), PutOption.PartitionKey(key)));
                            sendFuture.whenComplete((result, error) -> {
                                if (error != null) {
                                    log.warn("[{}] receive put with sequence keys error. {}", key, error.getMessage());
                                    return;
                                }
                                final String deltaKey = result.key();
                                final String[] deltas = deltaKey.split("-");
                                final long deltaL = Long.parseLong(deltas[1]);
                                final long deltaM = Long.parseLong(deltas[2]);
                                final long deltaR = Long.parseLong(deltas[3]);
                                if (deltaL != expectDeltaL.longValue()) {
                                    log.warn("[{}] detected unexpected delta(L)  expect: {} , actual: {}", key, expectDeltaL.get(), deltaL);
                                    expectDeltaL.set(deltaL);
                                }
                                if (deltaM != expectDeltaM.longValue()) {
                                    log.warn("[{}] detected unexpected delta(M):  expect: {} , actual: {}", key, expectDeltaM.get(), deltaM);
                                    expectDeltaM.set(deltaM);
                                }
                                if (deltaR != expectDeltaR.longValue()) {
                                    log.warn("[{}] detected unexpected delta(R):  expect: {} , actual: {}", key, expectDeltaR.get(), deltaR);
                                    expectDeltaR.set(deltaR);
                                }
                                expectDeltaL.addAndGet(1);
                                expectDeltaM.addAndGet(2);
                                expectDeltaM.addAndGet(3);
                            });
                        }
                    });
        }
        LockSupport.park();
    }
}
