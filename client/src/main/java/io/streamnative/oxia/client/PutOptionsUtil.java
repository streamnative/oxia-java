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

import io.streamnative.oxia.client.api.OptionEphemeral;
import io.streamnative.oxia.client.api.OptionVersionId;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.Version;
import java.util.OptionalLong;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
public class PutOptionsUtil {

    public static OptionalLong getVersionId(Set<PutOption> options) {
        if (options == null || options.isEmpty()) {
            return OptionalLong.empty();
        }

        boolean alreadyHasVersionId = false;
        long versionId = Version.KeyNotExists;
        for (PutOption o : options) {
            if (o instanceof OptionVersionId e) {
                if (alreadyHasVersionId) {
                    throw new IllegalArgumentException(
                            "Incompatible " + PutOption.class.getSimpleName() + "s: " + options);
                }

                versionId = e.versionId();
                alreadyHasVersionId = true;
            }
        }

        if (alreadyHasVersionId) {
            return OptionalLong.of(versionId);
        } else {
            return OptionalLong.empty();
        }
    }

    public static boolean isEphemeral(Set<PutOption> options) {
        if (options.isEmpty()) {
            return false;
        }

        for (PutOption option : options) {
            if (option instanceof OptionEphemeral) {
                return true;
            }
        }

        return false;
    }
}
