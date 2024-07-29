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
package io.streamnative.oxia.client.perf.ycsb.generator;

import static java.util.Objects.requireNonNull;

import java.util.Locale;

public enum GeneratorType {
    UNIFORM,
    ZIPFIAN,
    SEQUENTIAL;

    public static GeneratorType fromString(String type) {
        requireNonNull(type);
        for (GeneratorType gType : GeneratorType.values()) {
            if (gType.name().toLowerCase(Locale.ROOT).equals(type.toLowerCase(Locale.ROOT))) {
                return gType;
            }
        }
        throw new IllegalArgumentException("unknown generator type:" + type);
    }
}
