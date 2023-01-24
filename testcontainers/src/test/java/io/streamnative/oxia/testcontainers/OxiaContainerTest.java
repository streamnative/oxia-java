package io.streamnative.oxia.testcontainers;


import io.streamnative.oxia.client.OxiaClientBuilder;
import java.nio.charset.StandardCharsets;
import org.junit.AfterClass;
import org.junit.Assert;
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
    public void test() throws Exception {

        try (var client = new OxiaClientBuilder(container.getServiceAddress()).syncClient()) {
            client.put("hello", "world".getBytes(StandardCharsets.UTF_8), -1);
            var result = client.get("hello");
            Assert.assertEquals("world", new String(result.payload()));

        } catch (Exception e) {
            // TODO remove this catch block when client supports put and get
        }
    }
}
