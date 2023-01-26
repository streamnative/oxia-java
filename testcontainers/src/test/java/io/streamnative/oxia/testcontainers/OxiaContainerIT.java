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
package io.streamnative.oxia.testcontainers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.Network;

public class OxiaContainerIT {

    private static final String HOST = "oxia";
    private static OxiaContainer container;

    @BeforeClass
    public static void setupOxia() {
        container =
                new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases(HOST);
        container.start();
    }

    @AfterClass
    public static void tearDownOxia() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    public void testPutGetWithCLI() throws Exception {
        var clientContainer =
                new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME).withNetwork(container.getNetwork());
        var address = HOST + ":" + OxiaContainer.OXIA_PORT;
        try {
            clientContainer.start();
            var result =
                    clientContainer.execInContainer(
                            "oxia", "-a", address, "client", "put", "-k", "hello", "-v", "world");
            assertEquals("Put should create the record. Output: " + result, 0, result.getExitCode());
            result =
                    clientContainer.execInContainer("oxia", "-a", address, "client", "get", "-k", "hello");
            assertEquals(0, result.getExitCode());
            var stdOut = result.getStdout();
            assertTrue(
                    "Get should retrieve the value put before. Output: " + stdOut, stdOut.contains("world"));
        } finally {
            clientContainer.stop();
        }
    }

    @Test
    public void testMetricsUrl() throws Exception {
        var httpClient = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create(container.getMetricsUrl())).build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("oxia"));
    }
}
