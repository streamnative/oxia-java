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

import static io.streamnative.oxia.client.lock.SharedSimpleLock.DEFAULT_RETRYABLE_EXCEPTIONS;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.streamnative.oxia.client.api.AsyncLock;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.LockManager;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.OptionAutoRevalidate;
import io.streamnative.oxia.client.api.OptionBackoff;
import io.streamnative.oxia.client.metrics.Unit;
import io.streamnative.oxia.client.util.Backoff;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

final class LockManagerImpl implements LockManager, Consumer<Notification> {
    private final AsyncOxiaClient client;
    private final Map<String, AsyncLock> locks;
    private final ScheduledExecutorService executor;
    private final OptionAutoRevalidate optionAutoRevalidate;
    private final ObservableLongGauge gaugeOxiaLocksStatus;

    LockManagerImpl(
            AsyncOxiaClient client,
            Meter meter,
            ScheduledExecutorService scheduledExecutorService,
            OptionAutoRevalidate optionAutoRevalidate) {
        this.client = client;
        this.locks = new ConcurrentHashMap<>();
        this.executor = scheduledExecutorService;
        this.optionAutoRevalidate = optionAutoRevalidate;
        // register self as the notification receiver
        client.notifications(this);
        gaugeOxiaLocksStatus =
                meter
                        .gaugeBuilder("oxia.locks.status")
                        .setDescription("Current lock status")
                        .setUnit(Unit.Events.toString())
                        .ofLongs()
                        .buildWithCallback(
                                (ob) -> {
                                    final Set<Map.Entry<String, AsyncLock>> entries = locks.entrySet();
                                    for (Map.Entry<String, AsyncLock> entry : entries) {
                                        ob.record(
                                                1,
                                                Attributes.builder()
                                                        .put("oxia.lock.key", entry.getKey())
                                                        .put("oxia.lock.client.id", client.getClientIdentifier())
                                                        .put("oxia.lock.status", entry.getValue().getStatus().name())
                                                        .build());
                                    }
                                });
    }

    @Override
    public AsyncLock getSharedLock(String key, OptionBackoff optionBackoff) {
        return locks.computeIfAbsent(
                key,
                (k) ->
                        new SharedSimpleLock(
                                client,
                                key,
                                executor,
                                new Backoff(
                                        optionBackoff.initDelay(),
                                        optionBackoff.initDelayUnit(),
                                        optionBackoff.maxDelay(),
                                        optionBackoff.maxDelayUnit(),
                                        null),
                                optionAutoRevalidate,
                                DEFAULT_RETRYABLE_EXCEPTIONS));
    }

    @Override
    public AsyncLock getThreadSimpleLock(String key, OptionBackoff optionBackoff) {
        return locks.computeIfAbsent(
                key,
                (k) ->
                        new ThreadSimpleLock(
                                client,
                                key,
                                executor,
                                new Backoff(
                                        optionBackoff.initDelay(),
                                        optionBackoff.initDelayUnit(),
                                        optionBackoff.maxDelay(),
                                        optionBackoff.maxDelayUnit(),
                                        null),
                                optionAutoRevalidate,
                                DEFAULT_RETRYABLE_EXCEPTIONS));
    }

    @Override
    public void accept(Notification notification) {
        final var lock = locks.get(notification.key());
        if (lock == null) {
            return;
        }
        if (lock instanceof NotificationReceiver receiver) {
            receiver.notifyStateChanged(notification);
        }
    }

    @Override
    public void close() {
        gaugeOxiaLocksStatus.close();
    }
}
