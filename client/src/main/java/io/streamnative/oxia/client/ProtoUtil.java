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

import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.Version;
import io.streamnative.oxia.proto.GetResponse;
import io.streamnative.oxia.proto.PutResponse;
import java.nio.ByteBuffer;
import java.util.Optional;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ProtoUtil {

    public static int longToUint32(long value) {
        return ByteBuffer.allocate(8).putLong(value).position(4).getInt();
    }

    public static long uint32ToLong(int unit32AsInt) {
        return ByteBuffer.allocate(8).putInt(0).putInt(unit32AsInt).flip().getLong();
    }

    public static @NonNull PutResult getPutResultFromProto(@NonNull PutResponse response) {
        return new PutResult(getVersionFromProto(response.getVersion()));
    }

    public static @NonNull GetResult getResultFromProto(@NonNull GetResponse response) {
        return new GetResult(
                response.getValue().toByteArray(), getVersionFromProto(response.getVersion()));
    }

    public static @NonNull Version getVersionFromProto(
            @NonNull io.streamnative.oxia.proto.Version version) {
        return new Version(
                version.getVersionId(),
                version.getCreatedTimestamp(),
                version.getModifiedTimestamp(),
                version.getModificationsCount(),
                version.hasSessionId() ? Optional.of(version.getSessionId()) : Optional.empty(),
                version.hasClientIdentity() ? Optional.of(version.getClientIdentity()) : Optional.empty());
    }
}
