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
package io.streamnative.oxia.client.notify;

import static io.streamnative.oxia.client.api.Notification.KeyModified;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static lombok.AccessLevel.PACKAGE;

import io.streamnative.oxia.client.ProtoUtil;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.grpc.ChannelManager.StubFactory;
import io.streamnative.oxia.client.grpc.GrpcResponseStream;
import io.streamnative.oxia.proto.NotificationBatch;
import io.streamnative.oxia.proto.NotificationsRequest;
import io.streamnative.oxia.proto.ReactorOxiaClientGrpc.ReactorOxiaClientStub;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

@Slf4j
public class ShardNotificationReceiver extends GrpcResponseStream {

    @Getter(PACKAGE)
    private final long shardId;

    private final @NonNull Consumer<Notification> callback;
    private @NonNull Optional<Long> startingOffset = Optional.empty();

    private long offset;

    ShardNotificationReceiver(
            @NonNull Supplier<ReactorOxiaClientStub> stubFactory,
            long shardId,
            @NonNull Consumer<Notification> callback) {
        super(stubFactory);
        this.shardId = shardId;
        this.callback = callback;
    }

    public void start(@NonNull Optional<Long> offset) {
        startingOffset = offset;
        this.start();
    }

    @Override
    protected @NonNull CompletableFuture<Void> start(
            @NonNull ReactorOxiaClientStub stub, @NonNull Consumer<Disposable> consumer) {
        var request = NotificationsRequest.newBuilder().setShardId(ProtoUtil.longToUint32(shardId));
        startingOffset.ifPresent(o -> request.setStartOffsetExclusive(ProtoUtil.longToUint32(o)));
        // TODO filter non-retriables?
        RetryBackoffSpec retrySpec =
                Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100))
                        .doBeforeRetry(
                                signal ->
                                        log.warn("Retrying receiving notifications for shard {}: {}", shardId, signal));
        var disposable =
                stub.getNotifications(request.build())
                        .doOnError(t -> log.warn("Error receiving notifications for shard {}", shardId, t))
                        .retryWhen(retrySpec)
                        .repeat()
                        .publishOn(Schedulers.newSingle("shard-" + shardId + "-notifications"))
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
        private final @NonNull StubFactory<ReactorOxiaClientStub> reactorStubFactory;
        private final @NonNull Consumer<Notification> callback;

        @NonNull
        ShardNotificationReceiver newReceiver(long shardId, @NonNull String leader) {
            return new ShardNotificationReceiver(
                    () -> reactorStubFactory.apply(leader), shardId, callback);
        }
    }
}
