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
import static io.streamnative.oxia.client.api.PutOption.IfVersionIdEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.DeleteOption;
import io.streamnative.oxia.client.api.DeleteRangeOption;
import io.streamnative.oxia.client.api.GetOption;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.ListOption;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.Notification.KeyCreated;
import io.streamnative.oxia.client.api.Notification.KeyDeleted;
import io.streamnative.oxia.client.api.Notification.KeyModified;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.client.api.PutOption;
import io.streamnative.oxia.client.api.PutResult;
import io.streamnative.oxia.client.api.RangeScanOption;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import io.streamnative.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.streamnative.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Cleanup;
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
                    .withShards(10)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    private static AsyncOxiaClient client;

    private static Queue<Notification> notifications = new LinkedBlockingQueue<>();

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
                OxiaClientBuilder.create(oxia.getServiceAddress())
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
        var a = client.put("a", "a".getBytes(UTF_8), Set.of(IfRecordDoesNotExist));
        var b = client.put("b", "b".getBytes(UTF_8), Set.of(IfRecordDoesNotExist));
        var c = client.put("c", "c".getBytes(UTF_8));
        var d = client.put("d", "d".getBytes(UTF_8));
        allOf(a, b, c, d).join();

        assertThatThrownBy(
                        () -> client.put("a", "a".getBytes(UTF_8), Set.of(IfRecordDoesNotExist)).join())
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
        client.put("a", "a2".getBytes(UTF_8), Set.of(IfVersionIdEquals(aVersion))).join();
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
                        () ->
                                client
                                        .put("b", "b2".getBytes(UTF_8), Set.of(IfVersionIdEquals(bVersion + 1L)))
                                        .join())
                .hasCauseInstanceOf(UnexpectedVersionIdException.class);

        // delete with unexpected version
        var cVersion = client.get("c").join().getVersion().versionId();
        assertThatThrownBy(
                        () -> client.delete("c", Set.of(DeleteOption.IfVersionIdEquals(cVersion + 1L))).join())
                .hasCauseInstanceOf(UnexpectedVersionIdException.class);

        // list all keys
        var listResult = client.list("a", "e").join();
        assertThat(listResult).containsOnly("a", "b", "c", "d");

        // delete 'a' with expected version
        client.delete("a", Set.of(DeleteOption.IfVersionIdEquals(aVersion))).join();
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
                OxiaClientBuilder.create(oxia.getServiceAddress())
                        .clientIdentifier(identity)
                        .asyncClient()
                        .join()) {
            otherClient.put("f", "f".getBytes(), Set.of(PutOption.AsEphemeralRecord)).join();
            getResult = client.get("f").join();
            var sessionId = getResult.getVersion().sessionId().get();
            assertThat(sessionId).isNotNull();
            assertThat(getResult.getVersion().clientIdentifier().get()).isEqualTo(identity);

            var putResult =
                    otherClient.put("g", "g".getBytes(), Set.of(PutOption.AsEphemeralRecord)).join();
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

    @Test
    void testGetFloorCeiling() throws Exception {
        @Cleanup
        SyncOxiaClient client = OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient();

        client.put("a", "0".getBytes());
        // client.put("b", "1".getBytes()); // Skipped intentionally
        client.put("c", "2".getBytes());
        client.put("d", "3".getBytes());
        client.put("e", "4".getBytes());
        // client.put("f", "5".getBytes()); // Skipped intentionally
        client.put("g", "6".getBytes());

        GetResult gr = client.get("a");
        assertThat(gr.getKey()).isEqualTo("a");
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        gr = client.get("a", Collections.singleton(GetOption.ComparisonEqual));
        assertThat(gr.getKey()).isEqualTo("a");
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        gr = client.get("a", Collections.singleton(GetOption.ComparisonFloor));
        assertThat(gr.getKey()).isEqualTo("a");
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        gr = client.get("a", Collections.singleton(GetOption.ComparisonCeiling));
        assertThat(gr.getKey()).isEqualTo("a");
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        gr = client.get("a", Collections.singleton(GetOption.ComparisonLower));
        assertThat(gr).isNull();

        gr = client.get("a", Collections.singleton(GetOption.ComparisonHigher));
        assertThat(gr.getKey()).isEqualTo("c");
        assertThat(gr.getValue()).isEqualTo("2".getBytes());

        // ------------------------------------------------------------------------------------------------

        gr = client.get("b");
        assertThat(gr).isNull();

        gr = client.get("b", Collections.singleton(GetOption.ComparisonEqual));
        assertThat(gr).isNull();

        gr = client.get("b", Collections.singleton(GetOption.ComparisonFloor));
        assertThat(gr.getKey()).isEqualTo("a");
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        gr = client.get("b", Collections.singleton(GetOption.ComparisonCeiling));
        assertThat(gr.getKey()).isEqualTo("c");
        assertThat(gr.getValue()).isEqualTo("2".getBytes());

        gr = client.get("b", Collections.singleton(GetOption.ComparisonLower));
        assertThat(gr.getKey()).isEqualTo("a");
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        gr = client.get("b", Collections.singleton(GetOption.ComparisonHigher));
        assertThat(gr.getKey()).isEqualTo("c");
        assertThat(gr.getValue()).isEqualTo("2".getBytes());

        // ------------------------------------------------------------------------------------------------

        gr = client.get("c");
        assertThat(gr.getKey()).isEqualTo("c");
        assertThat(gr.getValue()).isEqualTo("2".getBytes());

        gr = client.get("c", Collections.singleton(GetOption.ComparisonEqual));
        assertThat(gr.getKey()).isEqualTo("c");
        assertThat(gr.getValue()).isEqualTo("2".getBytes());

        gr = client.get("c", Collections.singleton(GetOption.ComparisonFloor));
        assertThat(gr.getKey()).isEqualTo("c");
        assertThat(gr.getValue()).isEqualTo("2".getBytes());

        gr = client.get("c", Collections.singleton(GetOption.ComparisonCeiling));
        assertThat(gr.getKey()).isEqualTo("c");
        assertThat(gr.getValue()).isEqualTo("2".getBytes());

        gr = client.get("c", Collections.singleton(GetOption.ComparisonLower));
        assertThat(gr.getKey()).isEqualTo("a");
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        gr = client.get("c", Collections.singleton(GetOption.ComparisonHigher));
        assertThat(gr.getKey()).isEqualTo("d");
        assertThat(gr.getValue()).isEqualTo("3".getBytes());

        // ------------------------------------------------------------------------------------------------

        gr = client.get("d");
        assertThat(gr.getKey()).isEqualTo("d");
        assertThat(gr.getValue()).isEqualTo("3".getBytes());

        gr = client.get("d", Collections.singleton(GetOption.ComparisonEqual));
        assertThat(gr.getKey()).isEqualTo("d");
        assertThat(gr.getValue()).isEqualTo("3".getBytes());

        gr = client.get("d", Collections.singleton(GetOption.ComparisonFloor));
        assertThat(gr.getKey()).isEqualTo("d");
        assertThat(gr.getValue()).isEqualTo("3".getBytes());

        gr = client.get("d", Collections.singleton(GetOption.ComparisonCeiling));
        assertThat(gr.getKey()).isEqualTo("d");
        assertThat(gr.getValue()).isEqualTo("3".getBytes());

        gr = client.get("d", Collections.singleton(GetOption.ComparisonLower));
        assertThat(gr.getKey()).isEqualTo("c");
        assertThat(gr.getValue()).isEqualTo("2".getBytes());

        gr = client.get("d", Collections.singleton(GetOption.ComparisonHigher));
        assertThat(gr.getKey()).isEqualTo("e");
        assertThat(gr.getValue()).isEqualTo("4".getBytes());

        // ------------------------------------------------------------------------------------------------

        gr = client.get("e");
        assertThat(gr.getKey()).isEqualTo("e");
        assertThat(gr.getValue()).isEqualTo("4".getBytes());

        gr = client.get("e", Collections.singleton(GetOption.ComparisonEqual));
        assertThat(gr.getKey()).isEqualTo("e");
        assertThat(gr.getValue()).isEqualTo("4".getBytes());

        gr = client.get("e", Collections.singleton(GetOption.ComparisonFloor));
        assertThat(gr.getKey()).isEqualTo("e");
        assertThat(gr.getValue()).isEqualTo("4".getBytes());

        gr = client.get("e", Collections.singleton(GetOption.ComparisonCeiling));
        assertThat(gr.getKey()).isEqualTo("e");
        assertThat(gr.getValue()).isEqualTo("4".getBytes());

        gr = client.get("e", Collections.singleton(GetOption.ComparisonLower));
        assertThat(gr.getKey()).isEqualTo("d");
        assertThat(gr.getValue()).isEqualTo("3".getBytes());

        gr = client.get("e", Collections.singleton(GetOption.ComparisonHigher));
        assertThat(gr.getKey()).isEqualTo("g");
        assertThat(gr.getValue()).isEqualTo("6".getBytes());

        // ------------------------------------------------------------------------------------------------

        gr = client.get("f");
        assertThat(gr).isNull();

        gr = client.get("f", Collections.singleton(GetOption.ComparisonEqual));
        assertThat(gr).isNull();

        gr = client.get("f", Collections.singleton(GetOption.ComparisonFloor));
        assertThat(gr.getKey()).isEqualTo("e");
        assertThat(gr.getValue()).isEqualTo("4".getBytes());

        gr = client.get("f", Collections.singleton(GetOption.ComparisonCeiling));
        assertThat(gr.getKey()).isEqualTo("g");
        assertThat(gr.getValue()).isEqualTo("6".getBytes());

        gr = client.get("f", Collections.singleton(GetOption.ComparisonLower));
        assertThat(gr.getKey()).isEqualTo("e");
        assertThat(gr.getValue()).isEqualTo("4".getBytes());

        gr = client.get("f", Collections.singleton(GetOption.ComparisonHigher));
        assertThat(gr.getKey()).isEqualTo("g");
        assertThat(gr.getValue()).isEqualTo("6".getBytes());
    }

    @Test
    void testPartitionKey() throws Exception {
        @Cleanup
        SyncOxiaClient client = OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient();

        client.put("pk_a", "0".getBytes(), Set.of(PutOption.PartitionKey("x")));

        GetResult gr = client.get("pk_a");
        assertThat(gr).isNull();

        gr = client.get("pk_a", Set.of(GetOption.PartitionKey("x")));
        assertThat(gr.getKey()).isEqualTo("pk_a");
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        Set<PutOption> partitionKey = Set.of(PutOption.PartitionKey("x"));
        client.put("pk_a", "0".getBytes(), partitionKey);
        client.put("pk_b", "1".getBytes(), partitionKey);
        client.put("pk_c", "2".getBytes(), partitionKey);
        client.put("pk_d", "3".getBytes(), partitionKey);
        client.put("pk_e", "4".getBytes(), partitionKey);
        client.put("pk_f", "5".getBytes(), partitionKey);
        client.put("pk_g", "6".getBytes(), partitionKey);

        // Listing must yield the same results
        List<String> keys = client.list("pk_a", "pk_d");
        assertThat(keys).containsExactly("pk_a", "pk_b", "pk_c");

        keys = client.list("pk_a", "pk_d", Set.of(ListOption.PartitionKey("x")));
        assertThat(keys).containsExactly("pk_a", "pk_b", "pk_c");

        // Searching with wrong partition-key will return empty list
        keys = client.list("pk_a", "pk_d", Set.of(ListOption.PartitionKey("wrong-partition-key")));
        assertThat(keys).isEmpty();

        // Delete with wrong partition key would fail
        boolean deleted =
                client.delete("pk_g", Set.of(DeleteOption.PartitionKey("wrong-partition-key")));
        assertThat(deleted).isFalse();

        deleted = client.delete("pk_g", Set.of(DeleteOption.PartitionKey("x")));
        assertThat(deleted).isTrue();

        // Get tests
        gr = client.get("pk_a", Set.of(GetOption.ComparisonHigher));
        assertThat(gr.getKey()).isEqualTo("pk_b");
        assertThat(gr.getValue()).isEqualTo("1".getBytes());

        gr = client.get("pk_a", Set.of(GetOption.ComparisonHigher, GetOption.PartitionKey("x")));
        assertThat(gr.getKey()).isEqualTo("pk_b");
        assertThat(gr.getValue()).isEqualTo("1".getBytes());

        gr =
                client.get(
                        "pk_a",
                        Set.of(
                                GetOption.ComparisonHigher, GetOption.PartitionKey("another-wrong-partition-key")));
        assertThat(gr.getKey()).isNotEqualTo("pk_b");
        assertThat(gr.getValue()).isNotEqualTo("1".getBytes());

        // Delete with wrong partition key would fail to delete all keys
        client.deleteRange(
                "pk_c", "pk_e", Set.of(DeleteRangeOption.PartitionKey("wrong-partition-key")));

        keys = client.list("pk_c", "pk_f");
        assertThat(keys).containsExactly("pk_c", "pk_d", "pk_e");

        client.deleteRange("pk_c", "pk_e", Set.of(DeleteRangeOption.PartitionKey("x")));

        keys = client.list("pk_c", "pk_f");
        assertThat(keys).containsExactly("pk_e");
    }

    @Test
    void testSequentialKeys() throws Exception {
        @Cleanup
        SyncOxiaClient client = OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient();

        assertThatThrownBy(
                        () ->
                                client.put(
                                        "sk_a", "0".getBytes(), Set.of(PutOption.SequenceKeysDeltas(List.of(1L)))))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                client.put(
                                        "sk_a",
                                        "0".getBytes(),
                                        Set.of(PutOption.SequenceKeysDeltas(List.of(0L)), PutOption.PartitionKey("x"))))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                client.put(
                                        "sk_a",
                                        "0".getBytes(),
                                        Set.of(
                                                PutOption.SequenceKeysDeltas(List.of(1L, -1L)),
                                                PutOption.PartitionKey("x"))))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                client.put(
                                        "sk_a",
                                        "0".getBytes(),
                                        Set.of(
                                                PutOption.SequenceKeysDeltas(List.of(1L)),
                                                PutOption.PartitionKey("x"),
                                                PutOption.IfVersionIdEquals(1L))))
                .isInstanceOf(IllegalArgumentException.class);

        // Positive case scenarios
        PutResult pr =
                client.put(
                        "sk_a",
                        "0".getBytes(),
                        Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(1L))));
        assertThat(pr.key()).isEqualTo(String.format("sk_a-%020d", 1));

        pr =
                client.put(
                        "sk_a",
                        "1".getBytes(),
                        Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(3L))));
        assertThat(pr.key()).isEqualTo(String.format("sk_a-%020d", 4));

        pr =
                client.put(
                        "sk_a",
                        "2".getBytes(),
                        Set.of(PutOption.PartitionKey("x"), PutOption.SequenceKeysDeltas(List.of(1L, 6L))));
        assertThat(pr.key()).isEqualTo(String.format("sk_a-%020d-%020d", 5, 6));

        GetResult gr = client.get("sk_a", Set.of(GetOption.PartitionKey("x")));
        assertThat(gr).isNull();

        gr = client.get(String.format("sk_a-%020d", 1), Set.of(GetOption.PartitionKey("x")));
        assertThat(gr.getValue()).isEqualTo("0".getBytes());

        gr = client.get(String.format("sk_a-%020d", 4), Set.of(GetOption.PartitionKey("x")));
        assertThat(gr.getValue()).isEqualTo("1".getBytes());

        gr = client.get(String.format("sk_a-%020d-%020d", 5, 6), Set.of(GetOption.PartitionKey("x")));
        assertThat(gr.getValue()).isEqualTo("2".getBytes());
    }

    @Test
    void testRangeScanWithPartitionKey() throws Exception {
        @Cleanup
        SyncOxiaClient client = OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient();

        client.put("range-scan-pkey-a", "0".getBytes(), Set.of(PutOption.PartitionKey("x")));
        client.put("range-scan-pkey-b", "1".getBytes(), Set.of(PutOption.PartitionKey("x")));
        client.put("range-scan-pkey-c", "2".getBytes(), Set.of(PutOption.PartitionKey("x")));
        client.put("range-scan-pkey-d", "3".getBytes(), Set.of(PutOption.PartitionKey("x")));
        client.put("range-scan-pkey-e", "4".getBytes(), Set.of(PutOption.PartitionKey("x")));
        client.put("range-scan-pkey-f", "5".getBytes(), Set.of(PutOption.PartitionKey("x")));
        client.put("range-scan-pkey-g", "6".getBytes(), Set.of(PutOption.PartitionKey("x")));

        Iterable<GetResult> iterable =
                client.rangeScan(
                        "range-scan-pkey-a", "range-scan-pkey-d", Set.of(RangeScanOption.PartitionKey("x")));

        List<String> gr1List =
                StreamSupport.stream(iterable.spliterator(), false).map(GetResult::getKey).toList();
        assertThat(gr1List)
                .containsExactly("range-scan-pkey-a", "range-scan-pkey-b", "range-scan-pkey-c");
    }

    @Test
    void testRangeScan() throws Exception {
        @Cleanup
        SyncOxiaClient client = OxiaClientBuilder.create(oxia.getServiceAddress()).syncClient();

        client.put("range-scan-a", "0".getBytes());
        client.put("range-scan-b", "1".getBytes());
        client.put("range-scan-c", "2".getBytes());
        client.put("range-scan-d", "3".getBytes());
        client.put("range-scan-e", "4".getBytes());
        client.put("range-scan-f", "5".getBytes());
        client.put("range-scan-g", "6".getBytes());

        Iterable<GetResult> iterable = client.rangeScan("range-scan-a", "range-scan-d");

        List<String> list =
                StreamSupport.stream(iterable.spliterator(), false)
                        .map(GetResult::getKey)
                        .sorted()
                        .toList();
        assertThat(list).containsExactly("range-scan-a", "range-scan-b", "range-scan-c");

        // Check that the same iterable object can be used multiple times
        List<String> list2 =
                StreamSupport.stream(iterable.spliterator(), false)
                        .map(GetResult::getKey)
                        .sorted()
                        .toList();
        assertThat(list2).isEqualTo(list);
    }

    @Test
    void testVersionIdUniqueWithMultipleClient() throws Exception {
        final String path = "/testVersionIdUnique";
        final byte[] value = new byte[0];
        @Cleanup var client1 = OxiaClientBuilder.create(oxia.getServiceAddress()).asyncClient().join();
        @Cleanup var client2 = OxiaClientBuilder.create(oxia.getServiceAddress()).asyncClient().join();
        @Cleanup("shutdown")
        ExecutorService executor1 = Executors.newSingleThreadExecutor();

        @Cleanup("shutdown")
        ExecutorService executor2 = Executors.newSingleThreadExecutor();

        // :client-1
        List<CompletableFuture<PutResult>> r1 = new ArrayList<>();
        final CountDownLatch cdl1 = new CountDownLatch(1);
        executor1.execute(
                () -> {
                    for (int i = 0; i < 10; i++) {
                        client1.put(path, value);
                        client1.put(path, value);
                        r1.add(client1.put(path, value, Set.of(PutOption.AsNonBatchRecord)));
                        client1.put(path, value);
                        client1.put(path, value);
                        cdl1.countDown();
                    }
                });
        // :client-2
        List<CompletableFuture<PutResult>> r2 = new ArrayList<>();
        final CountDownLatch cdl2 = new CountDownLatch(1);
        executor2.execute(
                () -> {
                    for (int i = 0; i < 10; i++) {
                        client2.put(path, value);
                        client2.put(path, value);
                        r2.add(client2.put(path, value, Set.of(PutOption.AsNonBatchRecord)));
                        client2.put(path, value);
                        client2.put(path, value);
                        cdl2.countDown();
                    }
                });

        cdl1.await();
        CompletableFuture.allOf(r1.toArray(new CompletableFuture[0])).get();
        Set<Long> v1 = new HashSet<>();
        for (CompletableFuture<PutResult> result : r1) {
            v1.add(result.get().version().versionId());
        }
        assertEquals(10, v1.size());

        cdl2.await();
        CompletableFuture.allOf(r2.toArray(new CompletableFuture[0])).get();
        Set<Long> v2 = new HashSet<>();
        for (CompletableFuture<PutResult> result : r2) {
            v2.add(result.get().version().versionId());
        }
        assertEquals(10, v2.size());
    }

    @Test
    void testVersionIdUnique() throws ExecutionException, InterruptedException {
        final String path = "/testVersionIdUnique";
        final byte[] value = new byte[0];
        List<CompletableFuture<PutResult>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            client.put(path, value);
            results.add(client.put(path, value, Set.of(PutOption.AsNonBatchRecord)));
            client.put(path, value);
        }

        CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).get();

        Set<Long> versionId = new HashSet<>();
        for (CompletableFuture<PutResult> result : results) {
            versionId.add(result.get().version().versionId());
        }

        assertEquals(10, versionId.size());
    }

    @Test
    public void testModificationCount() throws Exception {
        final String path = "/testModificationCount";
        final byte[] value = new byte[0];
        List<CompletableFuture<PutResult>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            results.add(client.put(path, value));
        }

        CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).join();

        Set<Long> modificationCountNumber = new HashSet<>();
        for (CompletableFuture<PutResult> result : results) {
            modificationCountNumber.add(result.get().version().modificationsCount());
        }
        assertEquals(10, modificationCountNumber.size());
    }

    @Test
    void testVersionIdUniqueWithDifferentValue() throws Exception {
        final String path = "/testVersionIdUniqueWithValue";
        List<CompletableFuture<PutResult>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            byte[] value = ("value" + i).getBytes();
            results.add(client.put(path, value, Set.of(PutOption.AsNonBatchRecord)));
        }

        CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).get();

        Set<Long> versionId = new HashSet<>();
        for (CompletableFuture<PutResult> result : results) {
            versionId.add(result.get().version().versionId());
        }

        assertEquals(10, versionId.size());
    }

    @Test
    void testVersionIdUniqueWithOption() throws Exception {
        final String path = "/testVersionIdUniqueWithOption";
        final byte[] value = new byte[0];
        List<CompletableFuture<PutResult>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            results.add(
                    client.put(path, value, Set.of(PutOption.PartitionKey("x"), PutOption.AsNonBatchRecord)));
        }

        CompletableFuture.allOf(results.toArray(new CompletableFuture[0])).get();

        Set<Long> versionId = new HashSet<>();
        for (CompletableFuture<PutResult> result : results) {
            versionId.add(result.get().version().versionId());
        }

        assertEquals(10, versionId.size()); // failed, it always is 1
    }
}
