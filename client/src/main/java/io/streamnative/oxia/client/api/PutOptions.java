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

public record PutOptions(Long expectedVersionId, boolean ephemeral) {

    private static final PutOptions None = new Builder().build();

    public static Builder builder() {
        return new Builder();
    }

    public static PutOptions none() {
        return None;
    }

    public static PutOptions keyNotExists() {
        return new PutOptions(Version.KeyNotExists, false);
    }

    public static PutOptions expectedVersion(long versionId) {
        return new PutOptions(versionId, false);
    }

    public static class Builder {

        private Long versionId;
        //        private boolean ephemeral;

        Builder(Long versionId, boolean ephemeral) {
            if (ephemeral) {
                throw new UnsupportedOperationException();
            }
            this.versionId = versionId;
            //            this.ephemeral = ephemeral;
        }

        Builder() {
            this(null, false);
        }

        public PutOptions.Builder keyDoesNotExist() {
            versionId = Version.KeyNotExists;
            return this;
        }

        public PutOptions.Builder unconditional() {
            versionId = null;
            return this;
        }

        public PutOptions.Builder expectedVersionId(long versionId) {
            this.versionId = versionId;
            return this;
        }

        public PutOptions.Builder ephemeral() {
            throw new UnsupportedOperationException();
            //            ephemeral = true;
            //            return this;
        }

        public PutOptions build() {
            //            return new PutOptions(versionId, ephemeral);
            return new PutOptions(versionId, false);
        }
    }

    public Builder toBuilder() {
        return new Builder(expectedVersionId, ephemeral);
    }
}
