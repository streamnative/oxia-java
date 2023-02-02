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

import static io.streamnative.oxia.client.api.PutOption.EphemeralRecord;
import static io.streamnative.oxia.client.api.PutOption.VersionIdPutOption;
import static io.streamnative.oxia.client.api.PutOption.VersionIdPutOption.IfRecordDoesNotExist;
import static io.streamnative.oxia.client.api.PutOption.VersionIdPutOption.IfVersionIdEquals;
import static io.streamnative.oxia.client.api.PutOption.VersionIdPutOption.Unconditionally;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

public sealed interface PutOption permits VersionIdPutOption, EphemeralRecord {

    default boolean isVersionIdOption() {
        return false;
    }

    default boolean cannotCoExistWith(PutOption option) {
        return false;
    }

    sealed interface VersionIdPutOption extends PutOption
            permits IfVersionIdEquals, IfRecordDoesNotExist, Unconditionally {

        Long toVersionId();

        default boolean isVersionIdOption() {
            return true;
        }

        default boolean cannotCoExistWith(PutOption option) {
            return option instanceof VersionIdPutOption;
        }

        record IfVersionIdEquals(long versionId) implements VersionIdPutOption {
            @Override
            public Long toVersionId() {
                return versionId();
            }
        }

        record IfRecordDoesNotExist() implements VersionIdPutOption {
            @Override
            public Long toVersionId() {
                return Version.KeyNotExists;
            }
        }

        record Unconditionally() implements VersionIdPutOption {
            @Override
            public Long toVersionId() {
                return null;
            }
        }
    }

    record EphemeralRecord() implements PutOption {}

    VersionIdPutOption IfRecordDoesNotExist = new VersionIdPutOption.IfRecordDoesNotExist();
    VersionIdPutOption Unconditionally = new VersionIdPutOption.Unconditionally();
    PutOption EphemeralRecord = new EphemeralRecord();

    static VersionIdPutOption ifVersionIdEquals(long versionId) {
        return new IfVersionIdEquals(versionId);
    }

    static Set<PutOption> validate(PutOption... args) {
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
                                                + PutOption.class.getSimpleName()
                                                + "s: "
                                                + Arrays.toString(args));
                            }
                        });
        return Set.of(args);
    }

    static Long toVersionId(Collection<PutOption> options) {
        return options.stream()
                .filter(o -> o instanceof VersionIdPutOption)
                .findAny()
                .map(o -> ((VersionIdPutOption) o).toVersionId())
                .orElse(null);
    }

    static boolean toEphemeral(Collection<PutOption> options) {
        return options.stream().anyMatch(o -> o instanceof EphemeralRecord);
    }
}
