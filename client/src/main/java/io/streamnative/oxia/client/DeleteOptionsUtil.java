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

import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.OptionVersionId;
import java.util.OptionalLong;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DeleteOptionsUtil {

    public static OptionalLong getVersionId(Set<DeleteOption> opts) {
        if (opts == null || opts.isEmpty()) {
            return OptionalLong.empty();
        }

        if (opts.size() > 1) {
            throw new IllegalArgumentException("Conflicting delete options");
        }

        DeleteOption delOpt = opts.iterator().next();
        if (delOpt instanceof OptionVersionId o) {
            return OptionalLong.of(o.versionId());
        } else {
            return OptionalLong.empty();
        }
    }
}
