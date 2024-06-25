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
package io.streamnative.oxia.client.util;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import io.streamnative.oxia.client.api.Authentication;
import io.streamnative.oxia.client.api.AuthenticationType;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OxiaCredentialUtils {

    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY =
            Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);
    public static final String BEARER_TYPE = "Bearer";

    public static Metadata convertToOxiaCredentials(@Nonnull Authentication authentication) {
        if (authentication == null || authentication.generateCredentials().isEmpty()) {
            throw new IllegalArgumentException("The param authentication can not be null.");
        }
        Map<AuthenticationType, String> params = authentication.generateCredentials();
        if (params.size() > 1) {
            throw new IllegalArgumentException("Does not support multiple authentication types yet.");
        }
        Map.Entry<AuthenticationType, String> entry = params.entrySet().iterator().next();
        if (!entry.getKey().equals(AuthenticationType.BEARER)) {
            throw new IllegalArgumentException("Only support bearer authentication types at the moment.");
        }
        Metadata credentials = new Metadata();
        credentials.put(
                AUTHORIZATION_METADATA_KEY, String.format("%s %s", BEARER_TYPE, entry.getValue()));
        return credentials;
    }
}
