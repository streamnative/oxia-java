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
package io.streamnative.oxia.client.api;


import java.util.function.Consumer;
import lombok.NonNull;

/**
 * A contract for Oxia client builders to expose notification handling in a consistent manner.
 *
 * @param <B> The Type of client builder
 */
public interface ClientBuilder<B extends ClientBuilder> {
    /**
     * Registers a callback to receive Oxia notification.
     *
     * @param callback The callback.
     * @return The builder instance, for fluency.
     */
    @NonNull
    B notificationCallback(@NonNull Consumer<Notification> callback);
}
