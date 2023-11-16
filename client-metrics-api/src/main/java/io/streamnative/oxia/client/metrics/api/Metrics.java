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
package io.streamnative.oxia.client.metrics.api;

import java.util.Map;
import lombok.NonNull;

public interface Metrics {

    Metrics nullObject = NullObject.INSTANCE;

    Histogram histogram(String name, Unit unit);

    interface Histogram {
        void record(long value, Map<String, String> attributes);
    }

    enum Unit {
        NONE,
        BYTES,
        MILLISECONDS
    }

    static @NonNull Map<String, String> attributes(@NonNull String type, Throwable t) {
        return attributes(type, t == null);
    }

    static @NonNull Map<String, String> attributes(String type, boolean success) {
        return Map.of("type", type, "result", successOrFail(success));
    }

    static @NonNull Map<String, String> attributes(String type) {
        return attributes(type, true);
    }

    static @NonNull String successOrFail(boolean isSuccess) {
        return isSuccess ? "success" : "failure";
    }
}

enum NullObject implements Metrics {
    INSTANCE {
        @Override
        public Histogram histogram(String name, Unit unit) {
            return nullHistogram;
        }
    };

    final Histogram nullHistogram = (value, attributes) -> {};
}
