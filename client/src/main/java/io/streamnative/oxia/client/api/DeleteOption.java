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

import static io.streamnative.oxia.client.api.DeleteOption.VersionIdDeleteOption;
import static io.streamnative.oxia.client.api.DeleteOption.VersionIdDeleteOption.IfVersionIdEquals;
import static io.streamnative.oxia.client.api.DeleteOption.VersionIdDeleteOption.Unconditionally;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

public sealed interface DeleteOption permits VersionIdDeleteOption {

    default boolean cannotCoExistWith(DeleteOption option) {
        return false;
    }

    sealed interface VersionIdDeleteOption extends DeleteOption
            permits IfVersionIdEquals, Unconditionally {

        Long toVersionId();

        default boolean cannotCoExistWith(DeleteOption option) {
            return option instanceof VersionIdDeleteOption;
        }

        record IfVersionIdEquals(long versionId) implements VersionIdDeleteOption {
            @Override
            public Long toVersionId() {
                return versionId();
            }
        }

        record Unconditionally() implements VersionIdDeleteOption {
            @Override
            public Long toVersionId() {
                return null;
            }
        }
    }

    VersionIdDeleteOption Unconditionally = new VersionIdDeleteOption.Unconditionally();

    static VersionIdDeleteOption ifVersionIdEquals(long versionId) {
        return new IfVersionIdEquals(versionId);
    }

    static Set<DeleteOption> validate(DeleteOption... args) {
        if (args == null || args.length == 0) {
            return Set.of(Unconditionally);
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
        return Set.of(args);
    }

    static Long toVersionId(Collection<DeleteOption> options) {
        return options.stream()
                .filter(o -> o instanceof VersionIdDeleteOption)
                .findAny()
                .map(o -> ((VersionIdDeleteOption) o).toVersionId())
                .orElse(null);
    }
}
