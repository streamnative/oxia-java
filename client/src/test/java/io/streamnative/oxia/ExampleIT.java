package io.streamnative.oxia;


import io.streamnative.oxia.client.OxiaClientBuilder;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class ExampleIT extends IntegrationBase {

    @Test
    public void test() throws Exception {

        try (var client = new OxiaClientBuilder(getServiceAddress()).syncClient()) {
            client.put("hello", "world".getBytes(StandardCharsets.UTF_8), -1);
            var result = client.get("hello");
            Assert.assertEquals("world", new String(result.payload()));

        } catch (Exception e) {
            log.warn("", e);
            // TODO remove this catch block when client supports put and get
        }
    }
}
