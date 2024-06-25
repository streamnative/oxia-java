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
package io.streamnative.oxia.client.batch;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.streamnative.oxia.client.api.AuthenticationType;
import io.streamnative.oxia.client.util.OxiaCredentialUtils;
import java.util.Collections;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AuthBatchTest extends BatchTest {

    static {
        authentication = () -> Collections.singletonMap(AuthenticationType.BEARER, "123");
        serverInterceptor = new AuthInterceptor();
    }

    static class AuthInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata metadata, ServerCallHandler<ReqT, RespT> handler) {
            String token = metadata.get(OxiaCredentialUtils.AUTHORIZATION_METADATA_KEY);
            if (!"Bearer 123".equals(token)) {
                call.close(Status.UNAUTHENTICATED.withDescription("Token is wrong"), new Metadata());
                return new ServerCall.Listener<ReqT>() {};
            }
            return handler.startCall(call, metadata);
        }
    }
}
