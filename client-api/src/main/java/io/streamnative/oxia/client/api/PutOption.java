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
package io.streamnative.oxia.client.api;

import static io.streamnative.oxia.client.api.PutOption.AsEphemeralRecord;
import static io.streamnative.oxia.client.api.PutOption.VersionIdPutOption;
import static io.streamnative.oxia.client.api.PutOption.VersionIdPutOption.IfRecordDoesNotExist;
import static io.streamnative.oxia.client.api.PutOption.VersionIdPutOption.IfVersionIdEquals;
import static io.streamnative.oxia.client.api.PutOption.VersionIdPutOption.Unconditionally;

public sealed interface PutOption permits VersionIdPutOption, AsEphemeralRecord {

    default boolean cannotCoExistWith(PutOption option) {
        return false;
    }

    sealed interface VersionIdPutOption extends PutOption
            permits IfVersionIdEquals, IfRecordDoesNotExist, Unconditionally {

        Long toVersionId();

        default boolean cannotCoExistWith(PutOption option) {
            return option instanceof VersionIdPutOption;
        }

        record IfVersionIdEquals(long versionId) implements VersionIdPutOption {

            public IfVersionIdEquals {
                if (versionId < 0) {
                    throw new IllegalArgumentException("versionId cannot be less than 0 - was: " + versionId);
                }
            }

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

    record AsEphemeralRecord() implements PutOption {}

    VersionIdPutOption IfRecordDoesNotExist = new VersionIdPutOption.IfRecordDoesNotExist();
    VersionIdPutOption Unconditionally = new VersionIdPutOption.Unconditionally();
    PutOption AsEphemeralRecord = new AsEphemeralRecord();

    static VersionIdPutOption ifVersionIdEquals(long versionId) {
        return new IfVersionIdEquals(versionId);
    }
}
