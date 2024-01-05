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
package io.streamnative.oxia.client;

import static lombok.AccessLevel.PACKAGE;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.metrics.CacheMetrics;
import io.streamnative.oxia.client.metrics.api.Metrics;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

class CachingAsyncOxiaClient implements AsyncOxiaClient {
    private final @NonNull AsyncOxiaClient delegate;
    private final @NonNull AsyncLoadingCache<String, GetResult> recordCache;

    CachingAsyncOxiaClient(ClientConfig config, AsyncOxiaClient delegate) {
        this(delegate, new CacheFactory(config, delegate));
    }

    CachingAsyncOxiaClient(
            @NonNull AsyncOxiaClient delegate,
            @NonNull Supplier<AsyncLoadingCache<String, GetResult>> cacheFactory) {
        this.delegate = delegate;
        this.recordCache = cacheFactory.get();
        delegate.notifications(n -> recordCache.synchronous().invalidate(n.key()));
    }

    @Override
    public @NonNull CompletableFuture<PutResult> put(
            @NonNull String key, byte @NonNull [] value, @NonNull PutOption... options) {
        recordCache.synchronous().invalidate(key);
        return delegate.put(key, value, options);
    }

    @Override
    public @NonNull CompletableFuture<Boolean> delete(
            @NonNull String key, @NonNull DeleteOption... options) {
        recordCache.synchronous().invalidate(key);
        return delegate.delete(key, options);
    }

    @Override
    public @NonNull CompletableFuture<Void> deleteRange(
            @NonNull String startKeyInclusive, @NonNull String endKeyExclusive) {
        var cachedKeysInRange =
                recordCache.asMap().keySet().stream()
                        .filter(CompareWithSlash.withinRange(startKeyInclusive, endKeyExclusive))
                        .toList();
        recordCache.synchronous().invalidateAll(cachedKeysInRange);
        return delegate.deleteRange(startKeyInclusive, endKeyExclusive);
    }

    @Override
    public @NonNull CompletableFuture<GetResult> get(@NonNull String key) {
        return recordCache.get(key);
    }

    @Override
    public @NonNull CompletableFuture<List<String>> list(
            @NonNull String startKeyInclusive, @NonNull String endKeyExclusive) {
        return delegate.list(startKeyInclusive, endKeyExclusive);
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        delegate.notifications(notificationCallback);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    @RequiredArgsConstructor(access = PACKAGE)
    static class CacheFactory implements Supplier<AsyncLoadingCache<String, GetResult>> {
        private final @NonNull ClientConfig config;
        private final @NonNull AsyncOxiaClient delegate;
        private final @NonNull Supplier<CacheMetrics> cacheMetricsFactory;

        CacheFactory(ClientConfig config, AsyncOxiaClient delegate) {
            this(config, delegate, () -> CacheMetrics.create(config.metrics()));
        }

        @NonNull
        public AsyncLoadingCache<String, GetResult> get() {
            var builder = Caffeine.newBuilder().maximumSize(config.recordCacheCapacity());
            if (config.metrics() != Metrics.nullObject) {
                builder.recordStats(cacheMetricsFactory::get);
            }
            return builder.buildAsync((key, executor) -> delegate.get(key));
        }
    }
}
