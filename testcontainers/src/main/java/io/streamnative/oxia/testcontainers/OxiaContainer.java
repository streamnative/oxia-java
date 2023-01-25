package io.streamnative.oxia.testcontainers;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class OxiaContainer extends GenericContainer<OxiaContainer> {

    public static final int OXIA_PORT = 6648;
    public static final int METRICS_PORT = 8080;

    public static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("streamnative/oxia:main");

    public OxiaContainer(DockerImageName imageName) {
        super(imageName);
        this.withExposedPorts(OXIA_PORT, METRICS_PORT).withCommand("oxia", "standalone");
        this.waitStrategy =
                new HttpWaitStrategy()
                        .forPort(METRICS_PORT)
                        .forStatusCode(200)
                        .forPath("/metrics")
                        .withStartupTimeout(Duration.of(30, SECONDS));
    }

    public String getServiceAddress() {
        return getHost() + ":" + getMappedPort(OXIA_PORT);
    }

    public String getMetricsUrl() {
        return "http://" + getHost() + ":" + getMappedPort(METRICS_PORT) + "/metrics";
    }
}
