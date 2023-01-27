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

import static io.streamnative.oxia.client.api.AsyncOxiaClient.KeyNotExistsVersionId;
import static io.streamnative.oxia.testcontainers.OxiaContainer.DEFAULT_IMAGE_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.KeyAlreadyExistsException;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class OxiaClientIT {
    @Container private static final OxiaContainer oxia = new OxiaContainer(DEFAULT_IMAGE_NAME);

    private static AsyncOxiaClient client;

    @BeforeAll
    static void beforeAll() {
        client = new OxiaClientBuilder(oxia.getServiceAddress()).standalone().asyncClient();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void test() {
        var a = client.put("a", "a".getBytes(UTF_8), KeyNotExistsVersionId);
        var b = client.put("b", "b".getBytes(UTF_8), KeyNotExistsVersionId);
        var c = client.put("c", "c".getBytes(UTF_8));
        var d = client.put("d", "d".getBytes(UTF_8));
        allOf(a, b, c, d).join();

        assertThatThrownBy(() -> client.put("a", "a".getBytes(UTF_8), KeyNotExistsVersionId).join())
                .hasCauseInstanceOf(KeyAlreadyExistsException.class);
        // verify 'a' is present
        var getResult = client.get("a").join();
        assertThat(getResult.getValue()).isEqualTo("a".getBytes(UTF_8));
        var aVersion = getResult.getVersion().versionId();

        // update 'a' with expected version
        client.put("a", "a2".getBytes(UTF_8), aVersion).join();
        getResult = client.get("a").join();
        assertThat(getResult.getValue()).isEqualTo("a2".getBytes(UTF_8));
        aVersion = getResult.getVersion().versionId();

        // list all keys
        var listResult = client.list("a", "e").join();
        assertThat(listResult).containsExactly("a", "b", "c", "d");

        // delete 'a' with expected version
        client.delete("a", aVersion).join();
        getResult = client.get("a").join();
        assertThat(getResult).isNull();

        // delete 'b'
        client.delete("b").join();
        getResult = client.get("b").join();
        assertThat(getResult).isNull();

        // delete range (exclusive of 'd')
        client.deleteRange("c", "d").join();

        // list all keys
        listResult = client.list("a", "e").join();
        assertThat(listResult).containsExactly("d");
    }
}
