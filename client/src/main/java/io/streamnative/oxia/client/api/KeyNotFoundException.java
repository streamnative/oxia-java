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


import lombok.Getter;
import lombok.NonNull;

/** The key specified in the call did not exist on the server. */
public class KeyNotFoundException extends OxiaException {
    @Getter private final @NonNull String key;

    /**
     * Creates an instance of the exception.
     *
     * @param key The key specified in the call.
     */
    public KeyNotFoundException(@NonNull String key) {
        super("key not found: " + key);
        this.key = key;
    }
}
