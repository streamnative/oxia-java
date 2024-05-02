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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DeleteOptionsUtil {

    private static final Set<DeleteOption> DefaultDeleteOptions =
            Collections.singleton(DeleteOption.Unconditionally);

    public static Set<DeleteOption> validate(DeleteOption... args) {
        if (args == null || args.length == 0) {
            return DefaultDeleteOptions;
        }

        Arrays.stream(args)
                .forEach(
                        a -> {
                            if (Arrays.stream(args)
                                    .filter(c -> !c.equals(a))
                                    .anyMatch(c -> a.cannotCoExistWith(c))) {
                                throw new IllegalArgumentException(
                                        "Incompatible "
                                                + DeleteOption.class.getSimpleName()
                                                + "s: "
                                                + Arrays.toString(args));
                            }
                        });
        return new HashSet<>(Arrays.asList(args));
    }

    public static Optional<Long> toVersionId(Collection<DeleteOption> options) {
        return options.stream()
                .filter(o -> o instanceof DeleteOption.VersionIdDeleteOption)
                .findAny()
                .map(o -> ((DeleteOption.VersionIdDeleteOption) o).toVersionId());
    }
}
