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
package io.streamnative.oxia.client;


import io.streamnative.oxia.client.shard.ShardManager;
import io.streamnative.oxia.proto.OxiaClientGrpc.OxiaClientBlockingStub;
import java.util.function.Function;
import lombok.NonNull;

record BlockingStubByShardId(
        @NonNull ShardManager shardManager,
        @NonNull Function<String, OxiaClientBlockingStub> blockingStubFactory,
        @NonNull ClientConfig config)
        implements Function<Long, OxiaClientBlockingStub> {
    @Override
    public @NonNull OxiaClientBlockingStub apply(@NonNull Long shardId) {
        String leader;
        if (config.standalone()) {
            leader = config.serviceAddress();
        } else {
            leader = shardManager.leader(shardId);
        }
        return blockingStubFactory.apply(leader);
    }
}
