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
package io.streamnative.oxia.client.it;

import static io.streamnative.oxia.client.api.PutOption.IfRecordDoesNotExist;
import static io.streamnative.oxia.client.api.PutOption.ifVersionIdEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.streamnative.oxia.client.OxiaClientBuilder;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.api.Notification.KeyModified;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@Slf4j
public class OxiaClientIT {
    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME)
                    .withShards(4)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    private static AsyncOxiaClient client;

    private static List<Notification> notifications = new ArrayList<>();

    private static InMemoryMetricReader metricReader;

    @BeforeAll
    static void beforeAll() {
        Resource resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(ResourceAttributes.SERVICE_NAME, "logical-service-name")));

        metricReader = InMemoryMetricReader.create();
        SdkMeterProvider sdkMeterProvider =
                SdkMeterProvider.builder().registerMetricReader(metricReader).setResource(resource).build();

        OpenTelemetry openTelemetry =
                OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();

        client =
                new OxiaClientBuilder(oxia.getServiceAddress())
                        .openTelemetry(openTelemetry)
                        .asyncClient()
                        .join();
        client.notifications(notifications::add);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void test() throws Exception {
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

        var identity = getClass().getSimpleName();
        try (var otherClient =
                new OxiaClientBuilder(oxia.getServiceAddress())
                        .clientIdentifier(identity)
                        .asyncClient()
                        .join()) {
            otherClient.put("f", "f".getBytes(), PutOption.AsEphemeralRecord).join();
            getResult = client.get("f").join();
            var sessionId = getResult.getVersion().sessionId().get();
            assertThat(sessionId).isNotNull();
            assertThat(getResult.getVersion().clientIdentifier().get()).isEqualTo(identity);

            var putResult = otherClient.put("g", "g".getBytes(), PutOption.AsEphemeralRecord).join();
            assertThat(putResult.version().clientIdentifier().get()).isEqualTo(identity);
            assertThat(putResult.version().sessionId().get()).isNotNull();

            otherClient.put("h", "h".getBytes()).join();
        } // otherClient closed

        await()
                .untilAsserted(
                        () -> {
                            assertThat(client.get("f").join()).isNull();
                        });
        assertThat(client.get("g").join()).isNull();
        assertThat(client.get("h").join()).isNotNull();

        metricReader.forceFlush();
        var metrics = metricReader.collectAllMetrics();
        var metricsByName = metrics.stream().collect(Collectors.toMap(MetricData::getName, identity()));

        System.out.println(metricsByName);

        assertThat(
                        metricsByName.get("oxia.client.ops").getHistogramData().getPoints().stream()
                                .map(HistogramPointData::getCount)
                                .reduce(0L, Long::sum))
                .isEqualTo(24);
    }
}
