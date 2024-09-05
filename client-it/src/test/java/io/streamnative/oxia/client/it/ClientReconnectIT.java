package io.streamnative.oxia.client.it;

import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.GetResult;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Testcontainers
@Slf4j
public class ClientReconnectIT {

    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME, 4, true)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    @Test
    public void testReconnection() {
        final AsyncOxiaClient client = OxiaClientBuilder.create(oxia.getServiceAddress())
                .asyncClient().join();
        final String key = "1";
        final byte[] value = "1".getBytes(StandardCharsets.UTF_8);

        final long startTime = System.currentTimeMillis();
        final long elapse = 3000L;
        while (true) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            try {
                client.put(key, value).get(1, TimeUnit.SECONDS);
            } catch (Throwable ex) {
                Assertions.fail("unexpected behaviour", ex);
            }

            try {
                final GetResult getResult = client.get("1").get(1, TimeUnit.SECONDS);
                Assertions.assertArrayEquals(getResult.getValue(), value);
            } catch (Throwable ex) {
                Assertions.fail("unexpected behaviour", ex);
            }


            if (System.currentTimeMillis() - startTime >= elapse) {
                oxia.stop();

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                oxia.start();

                Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
                    try {
                        client.put(key, value).get(1, TimeUnit.SECONDS);
                    } catch (Throwable ex) {
                        Assertions.fail("unexpected behaviour", ex);
                    }

                    try {
                        final GetResult getResult = client.get("1").get(1, TimeUnit.SECONDS);
                        Assertions.assertArrayEquals(getResult.getValue(), value);
                    } catch (Throwable ex) {
                        Assertions.fail("unexpected behaviour", ex);
                    }
                });
                break;
            }
        }
    }
}
