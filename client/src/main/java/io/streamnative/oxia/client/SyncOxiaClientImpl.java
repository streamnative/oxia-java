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


import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.UnexpectedVersionIdException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class SyncOxiaClientImpl implements SyncOxiaClient {
    private final AsyncOxiaClient asyncClient;

    @SneakyThrows
    @Override
    public @NonNull PutResult put(@NonNull String key, byte @NonNull [] value, PutOption... options) {
        try {
            return asyncClient.put(key, value, options).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public boolean delete(@NonNull String key, DeleteOption... options)
            throws UnexpectedVersionIdException {
        try {
            return asyncClient.delete(key, options).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public void deleteRange(@NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        try {
            asyncClient.deleteRange(minKeyInclusive, maxKeyExclusive).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public GetResult get(@NonNull String key) {
        try {
            return asyncClient.get(key).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public @NonNull List<String> list(
            @NonNull String minKeyInclusive, @NonNull String maxKeyExclusive) {
        try {
            return asyncClient.list(minKeyInclusive, maxKeyExclusive).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        asyncClient.notifications(notificationCallback);
    }

    @Override
    public void close() throws Exception {
        asyncClient.close();
    }
}
