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
package io.streamnative.oxia.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.streamnative.oxia.client.CachingAsyncOxiaClient.CacheFactory;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.api.Notification.KeyModified;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.Version;
import io.streamnative.oxia.client.metrics.CacheMetrics;
import io.streamnative.oxia.client.metrics.api.Metrics;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CachingAsyncOxiaClientTest {

    @Mock AsyncOxiaClient delegate;
    @Mock AsyncLoadingCache<String, GetResult> cache;
    @Mock LoadingCache<String, GetResult> syncCache;
    @Mock ConcurrentMap<String, CompletableFuture<GetResult>> map;
    @Captor ArgumentCaptor<Consumer<Notification>> consumerCaptor;
    @Captor ArgumentCaptor<Iterable<String>> keyCaptor;

    CachingAsyncOxiaClient client;

    @BeforeEach
    void setUp() {
        client = new CachingAsyncOxiaClient(delegate, () -> cache);
    }

    @Test
    void put() {
        when(cache.synchronous()).thenReturn(syncCache);
        var value = "value".getBytes(UTF_8);
        var version = new Version(1L, 2L, 3L, 4L, Optional.empty(), Optional.empty());
        var result = CompletableFuture.completedFuture(new PutResult(version));
        when(delegate.put("a", value)).thenReturn(result);
        assertThat(client.put("a", value)).isSameAs(result);
        var inOrder = inOrder(delegate, syncCache);
        inOrder.verify(syncCache).invalidate("a");
        inOrder.verify(delegate).put("a", value);
    }

    @Test
    void delete() {
        when(cache.synchronous()).thenReturn(syncCache);
        var result = CompletableFuture.completedFuture(true);
        when(delegate.delete("a")).thenReturn(result);
        assertThat(client.delete("a")).isSameAs(result);
        var inOrder = inOrder(delegate, syncCache);
        inOrder.verify(syncCache).invalidate("a");
        inOrder.verify(delegate).delete("a");
    }

    @Test
    void deleteRange() {
        when(cache.synchronous()).thenReturn(syncCache);
        when(cache.asMap()).thenReturn(map);
        when(map.keySet()).thenReturn(Set.of("a", "d", "f", "z"));

        var result = CompletableFuture.<Void>completedFuture(null);
        when(delegate.deleteRange("b", "k")).thenReturn(result);
        assertThat(client.deleteRange("b", "k")).isSameAs(result);
        verify(syncCache).invalidateAll(keyCaptor.capture());

        Set<String> invalidatedKeys = new HashSet<>();
        keyCaptor.getValue().forEach(invalidatedKeys::add);

        assertThat(invalidatedKeys).containsExactlyInAnyOrder("d", "f");
    }

    @Test
    void getMocked() {
        var value = "value".getBytes(UTF_8);
        var version = new Version(1L, 2L, 3L, 4L, Optional.empty(), Optional.empty());
        var result = CompletableFuture.completedFuture(new GetResult(value, version));
        when(cache.get("a")).thenReturn(result);
        assertThat(client.get("a")).isSameAs(result);
    }

    @Test
    void get() throws Exception {
        var metrics = mock(Metrics.class);
        var cacheMetrics = mock(CacheMetrics.class);
        var config =
                new ClientConfig(
                        "localhost:8080", ZERO, ZERO, 1, 1024 * 1024, Optional.empty(), 1, ZERO, "id", metrics);
        var cacheFactory = new CacheFactory(config, delegate, () -> cacheMetrics);

        var value = "value".getBytes(UTF_8);
        var version = new Version(1L, 2L, 3L, 4L, Optional.empty(), Optional.empty());
        var get = new GetResult(value, version);
        var result = CompletableFuture.completedFuture(get);
        when(delegate.get("a")).thenReturn(result);
        client = new CachingAsyncOxiaClient(delegate, cacheFactory);
        assertThat(client.get("a").get()).isEqualTo(get);
        assertThat(client.get("a").get()).isEqualTo(get);
        assertThat(client.get("a").get()).isEqualTo(get);
        assertThat(client.get("a").get()).isEqualTo(get);
        assertThat(client.get("a").get()).isEqualTo(get);
        verify(delegate, times(1)).get("a");

        verify(cacheMetrics, times(1)).recordMisses(1);
        verify(cacheMetrics, times(1)).recordLoadSuccess(anyLong());
        verify(cacheMetrics, times(4)).recordHits(1);
    }

    @Test
    void list() {
        var result = CompletableFuture.completedFuture(List.of("x"));
        when(delegate.list("a", "b")).thenReturn(result);
        assertThat(client.list("a", "b")).isSameAs(result);
    }

    @Test
    void notifications() {
        Consumer<Notification> consumer = n -> {};
        client.notifications(consumer);
        verify(delegate).notifications(consumer);
    }

    @Test
    void notificationInvalidates() {
        when(cache.synchronous()).thenReturn(syncCache);
        verify(delegate).notifications(consumerCaptor.capture());
        var cacheNotificationConsumer = consumerCaptor.getValue();
        cacheNotificationConsumer.accept(new KeyDeleted("a"));
        cacheNotificationConsumer.accept(new KeyModified("b", 2));
        cacheNotificationConsumer.accept(new KeyCreated("c", 1));

        verify(syncCache).invalidate("a");
        verify(syncCache).invalidate("b");
        verify(syncCache).invalidate("c");
    }

    @Test
    void close() throws Exception {
        client.close();
        verify(delegate).close();
    }
}
