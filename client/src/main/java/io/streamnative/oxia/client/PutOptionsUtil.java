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

import io.streamnative.oxia.client.api.PutOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
public class PutOptionsUtil {

    private static final Set<PutOption> DefaultPutOptions =
            Collections.singleton(PutOption.Unconditionally);

    public static Optional<Long> toVersionId(Collection<PutOption> options) {
        return options.stream()
                .filter(o -> o instanceof PutOption.VersionIdPutOption)
                .findAny()
                .map(o -> ((PutOption.VersionIdPutOption) o).toVersionId());
    }

    public static boolean toEphemeral(Collection<PutOption> options) {
        return options.stream().anyMatch(o -> o instanceof PutOption.AsEphemeralRecord);
    }

    public static Set<PutOption> validate(PutOption... args) {
        if (args == null || args.length == 0) {
            return DefaultPutOptions;
        }
        Arrays.stream(args)
                .forEach(
                        a -> {
                            if (Arrays.stream(args)
                                    .filter(c -> !c.equals(a))
                                    .anyMatch(c -> a.cannotCoExistWith(c))) {
                                throw new IllegalArgumentException(
                                        "Incompatible "
                                                + PutOption.class.getSimpleName()
                                                + "s: "
                                                + Arrays.toString(args));
                            }
                        });
        return new HashSet<>(Arrays.asList(args));
    }
}
