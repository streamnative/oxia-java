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
package io.oxia.client.api;

import lombok.NonNull;
import lombok.Value;

/** The result of a client get request. */
@Value
public class GetResult {

    /** The key associated with the record. */
    @NonNull String key;

    /** The value associated with the key specified in the call. */
    @NonNull byte[] value;

    /** Metadata for the record associated with the key specified in the call. */
    @NonNull Version version;
}
