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
package io.streamnative.oxia.client.notify;

import static io.streamnative.oxia.client.api.Notification.KeyModified;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.grpc.OxiaStubManager;
import io.streamnative.oxia.client.metrics.NotificationMetrics;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

@Slf4j
public class ShardNotificationReceiver extends GrpcResponseStream {

    @Getter(PACKAGE)
    private final long shardId;

    private final @NonNull Consumer<Notification> callback;
    private final @NonNull NotificationMetrics metrics;
    private @NonNull Optional<Long> startingOffset = Optional.empty();

    private Scheduler scheduler;
    private long offset;

    ShardNotificationReceiver(
            @NonNull OxiaStub stub,
            long shardId,
            @NonNull Consumer<Notification> callback,
            @NonNull NotificationMetrics metrics) {
        super(stub);
        this.shardId = shardId;
        this.callback = callback;
        this.metrics = metrics;
    }

    public void start(@NonNull Optional<Long> offset) {
        if (offset.isPresent() && offset.get() < 0) {
            throw new IllegalArgumentException("Invalid offset: " + offset.get());
        }
        startingOffset = offset;
        this.start();
    }

    @Override
    public void close() {
        super.close();

        if (scheduler != null) {
            scheduler.dispose();
        }
    }

    @Override
    protected @NonNull CompletableFuture<Void> start(
            @NonNull OxiaStub stub, @NonNull Consumer<Disposable> consumer) {
        var request = NotificationsRequest.newBuilder().setShardId(shardId);
        startingOffset.ifPresent(o -> request.setStartOffsetExclusive(o));
        // TODO filter non-retriables?
        RetryBackoffSpec retrySpec =
                Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                        .doBeforeRetry(
                                signal ->
                                        log.warn("Retrying receiving notifications for shard {}: {}", shardId, signal));
        var threadName = String.format("shard-%s-notifications", shardId);
        scheduler = Schedulers.newSingle(threadName);
        var disposable =
                Flux.defer(() -> stub.reactor().getNotifications(request.build()))
                        .doOnError(t -> log.warn("Error receiving notifications for shard {}", shardId, t))
                        .doOnEach(metrics::recordBatch)
                        .retryWhen(retrySpec)
                        .repeat()
                        .publishOn(scheduler)
                        .subscribe(this::notify);
        consumer.accept(disposable);
        return completedFuture(null);
    }

    private void notify(@NonNull NotificationBatch batch) {
        offset = Math.max(batch.getOffset(), offset);
        batch.getNotificationsMap().entrySet().stream()
                .map(
                        e -> {
                            var key = e.getKey();
                            var notice = e.getValue();
                            return switch (notice.getType()) {
                                case KEY_CREATED -> new KeyCreated(key, notice.getVersionId());
                                case KEY_MODIFIED -> new KeyModified(key, notice.getVersionId());
                                case KEY_DELETED -> new KeyDeleted(key);
                                default -> null;
                            };
                        })
                .filter(Objects::nonNull)
                .forEach(callback::accept);
    }

    public long getOffset() {
        return offset;
    }

    @RequiredArgsConstructor(access = PACKAGE)
    static class Factory {
        private final @NonNull OxiaStubManager stubManager;
        private final @NonNull Consumer<Notification> callback;

        @NonNull
        ShardNotificationReceiver newReceiver(
                long shardId, @NonNull String leader, @NonNull NotificationMetrics metrics) {
            return new ShardNotificationReceiver(stubManager.getStub(leader), shardId, callback, metrics);
        }
    }
}
