package io.streamnative.oxia.testcontainers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("First we need to make sure we can access the docker image")
public class OxiaContainerTest {

    private static OxiaContainer container;

    @BeforeClass
    public static void setupOxia() {
        container = new OxiaContainer();
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
        var result =
                container.execInContainer("/oxia/bin/oxia", "client", "put", "-k", "hello", "-v", "world");
        assertEquals(0, result.getExitCode());
        result = container.execInContainer("/oxia/bin/oxia", "client", "get", "-k", "hello");
        assertEquals(0, result.getExitCode());
        var stdOut = result.getStdout();
        assertTrue(
                "Get should retrieve the value put before. Output: " + stdOut, stdOut.contains("world"));
    }
}
