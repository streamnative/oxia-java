package io.streamnative.oxia.testcontainers;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class OxiaContainer extends GenericContainer<OxiaContainer> {

    public static final int OXIA_PORT = 6648;
    public static final int METRICS_PORT = 8080;

    public static final String DEFAULT_IMAGE_NAME = "streamnative/oxia:main";

    private final String containerName;

    public OxiaContainer() {
        this(DockerImageName.parse(DEFAULT_IMAGE_NAME), "oxia-container");
    }

    public OxiaContainer(DockerImageName imageName, String containerName) {
        super(imageName);
        this.containerName = containerName;
    }

    @Override
    public String getContainerName() {
        return containerName;
    }

    @Override
    protected void configure() {
        super.configure();
        withExposedPorts(OXIA_PORT, METRICS_PORT);
    }

    @Override
    public void start() {
        this.waitStrategy =
                new HttpWaitStrategy()
                        .forPort(METRICS_PORT)
                        .forStatusCode(200)
                        .forPath("/metrics")
                        .withStartupTimeout(Duration.of(30, SECONDS));
        this.withCreateContainerCmdModifier(
                createContainerCmd -> {
                    createContainerCmd.withHostName(containerName);
                    createContainerCmd.withName(getContainerName());
                    createContainerCmd.withEntrypoint("/oxia/bin/oxia", "standalone");
                });

        super.start();
    }

    public String getServiceAddress() {
        return getHost() + ":" + getMappedPort(OXIA_PORT);
    }

    public String getMetricsUrl() {
        return "http://" + getHost() + ":" + getMappedPort(METRICS_PORT) + "/metrics";
    }
}
