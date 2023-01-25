package io.streamnative.oxia.testcontainers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.Network;

@Ignore("First we need to make sure we can access the docker image")
public class OxiaContainerTest {

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
