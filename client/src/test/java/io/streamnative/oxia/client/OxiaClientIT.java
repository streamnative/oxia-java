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

import static io.streamnative.oxia.client.api.PutOption.IfRecordDoesNotExist;
import static io.streamnative.oxia.client.api.PutOption.ifVersionIdEquals;
import static io.streamnative.oxia.testcontainers.OxiaContainer.DEFAULT_IMAGE_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.api.Notification.KeyModified;
import io.streamnative.oxia.client.api.UnexpectedVersionIdException;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class OxiaClientIT {
    @Container
    private static final OxiaContainer oxia = new OxiaContainer(DEFAULT_IMAGE_NAME).withShards(4);

    private static AsyncOxiaClient client;

    private static List<Notification> notifications = new ArrayList<>();

    @BeforeAll
    static void beforeAll() {
        client =
                new OxiaClientBuilder(oxia.getServiceAddress())
                        .notificationCallback(notifications::add)
                        .asyncClient()
                        .join();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void test() {
        var a = client.put("a", "a".getBytes(UTF_8), IfRecordDoesNotExist);
        var b = client.put("b", "b".getBytes(UTF_8), IfRecordDoesNotExist);
        var c = client.put("c", "c".getBytes(UTF_8));
        var d = client.put("d", "d".getBytes(UTF_8));
        allOf(a, b, c, d).join();

        assertThatThrownBy(() -> client.put("a", "a".getBytes(UTF_8), IfRecordDoesNotExist).join())
                .hasCauseInstanceOf(KeyAlreadyExistsException.class);
        // verify 'a' is present
        var getResult = client.get("a").join();
        assertThat(getResult.getValue()).isEqualTo("a".getBytes(UTF_8));
        var aVersion = getResult.getVersion().versionId();

        // verify notification for 'a'
        long finalAVersion = aVersion;
        await()
                .untilAsserted(
                        () -> assertThat(notifications).contains(new KeyCreated("a", finalAVersion)));

        // update 'a' with expected version
        client.put("a", "a2".getBytes(UTF_8), ifVersionIdEquals(aVersion)).join();
        getResult = client.get("a").join();
        assertThat(getResult.getValue()).isEqualTo("a2".getBytes(UTF_8));
        aVersion = getResult.getVersion().versionId();

        // verify notification for 'a' update
        long finalA2Version = aVersion;
        await()
                .untilAsserted(
                        () -> assertThat(notifications).contains(new KeyModified("a", finalA2Version)));

        // put with unexpected version
        var bVersion = client.get("b").join().getVersion().versionId();
        assertThatThrownBy(
                        () -> client.put("b", "b2".getBytes(UTF_8), ifVersionIdEquals(bVersion + 1L)).join())
                .hasCauseInstanceOf(UnexpectedVersionIdException.class);

        // delete with unexpected version
        var cVersion = client.get("c").join().getVersion().versionId();
        assertThatThrownBy(
                        () -> client.delete("c", DeleteOption.ifVersionIdEquals(cVersion + 1L)).join())
                .hasCauseInstanceOf(UnexpectedVersionIdException.class);

        // list all keys
        var listResult = client.list("a", "e").join();
        assertThat(listResult).containsOnly("a", "b", "c", "d");

        // delete 'a' with expected version
        client.delete("a", DeleteOption.ifVersionIdEquals(aVersion)).join();
        getResult = client.get("a").join();
        assertThat(getResult).isNull();

        // verify notification for 'a' update
        await().untilAsserted(() -> assertThat(notifications).contains(new KeyDeleted("a")));

        // delete 'b'
        client.delete("b").join();
        getResult = client.get("b").join();
        assertThat(getResult).isNull();

        // delete range (exclusive of 'd')
        client.deleteRange("c", "d").join();

        // list all keys
        listResult = client.list("a", "e").join();
        assertThat(listResult).containsExactly("d");

        // get non-existent key
        assertThat(client.get("z").join()).isNull();
    }
}
