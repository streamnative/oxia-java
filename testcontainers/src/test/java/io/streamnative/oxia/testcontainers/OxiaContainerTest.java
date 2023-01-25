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

@Ignore("First we need to make sure we can access the docker image")
public class OxiaContainerTest {

    private static OxiaContainer container;

    @BeforeClass
    public static void setupOxia() {
        container = new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME);
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
        var result = container.execInContainer("oxia", "client", "put", "-k", "hello", "-v", "world");
        assertEquals(0, result.getExitCode());
        result = container.execInContainer("oxia", "client", "get", "-k", "hello");
        assertEquals(0, result.getExitCode());
        var stdOut = result.getStdout();
        assertTrue(
                "Get should retrieve the value put before. Output: " + stdOut, stdOut.contains("world"));
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
