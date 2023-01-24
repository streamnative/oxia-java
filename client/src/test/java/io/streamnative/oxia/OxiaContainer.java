package io.streamnative.oxia;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class OxiaContainer extends GenericContainer<OxiaContainer> {

    public static final int OXIA_PORT = 6648;
    public static final int METRICS_PORT = 8080;

    public static final String DEFAULT_IMAGE_NAME =
            System.getenv().getOrDefault("OXIA_TEST_IMAGE_NAME", "oxia:latest");

    /**
     * For debugging purposes, it is useful to have the ability to leave containers running. This mode
     * can be activated by setting environment variables OXIA_CONTAINERS_LEAVE_RUNNING=true and
     * TESTCONTAINERS_REUSE_ENABLE=true After debugging, one can use this command to kill all
     * containers that were left running: docker kill $(docker ps -q --filter
     * "label=oxiacontainer=true")
     */
    public static final boolean OXIA_CONTAINERS_LEAVE_RUNNING =
            Boolean.getBoolean("OXIA_CONTAINERS_LEAVE_RUNNING");

    private final String containerName;

    public OxiaContainer() {
        this(DockerImageName.parse(DEFAULT_IMAGE_NAME), "oxia-container");
    }

    public OxiaContainer(DockerImageName imageName, String containerName) {
        super(imageName);
        this.containerName = containerName;
        configureLeaveContainerRunning(this);
    }

    public static void configureLeaveContainerRunning(GenericContainer<?> container) {
        if (OXIA_CONTAINERS_LEAVE_RUNNING) {
            // use Testcontainers reuse containers feature to leave the container running
            container.withReuse(true);
            // add label that can be used to find containers that are left running.
            container.withLabel("oxiacontainer", "true");
            // add a random label to prevent reuse of containers
            container.withLabel("oxiacontainer.random", UUID.randomUUID().toString());
        }
    }

    @Override
    public void stop() {
        if (OXIA_CONTAINERS_LEAVE_RUNNING) {
            log.warn("Ignoring stop due to OXIA_CONTAINERS_LEAVE_RUNNING=true.");
            return;
        }
        super.stop();
    }

    @Override
    public String getContainerName() {
        return containerName;
    }

    @Override
    protected void configure() {
        super.configure();
        withExposedPorts(OXIA_PORT, METRICS_PORT);
        withLogConsumer(
                o -> {
                    log.info(">>> {}", o.getUtf8String());
                });
    }

    @Override
    public void start() {
        this.waitStrategy =
                new HttpWaitStrategy()
                        .forPort(METRICS_PORT)
                        .forStatusCode(200)
                        .forPath("/metrics")
                        .withStartupTimeout(Duration.of(300, SECONDS));
        this.withCreateContainerCmdModifier(
                createContainerCmd -> {
                    createContainerCmd.withHostName(containerName);
                    createContainerCmd.withName(getContainerName());
                    createContainerCmd.withEntrypoint("/oxia/bin/oxia", "standalone");
                });

        super.start();
        log.info("[{}] Start oxia service at container {}", getContainerName(), getContainerId());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof OxiaContainer)) {
            return false;
        }

        OxiaContainer another = (OxiaContainer) o;
        return getContainerId().equals(another.getContainerId()) && super.equals(another);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(getContainerId());
    }

    public String getServiceAddress() {
        return getHost() + ":" + getMappedPort(OXIA_PORT);
    }

    public String getMetricsUrl() {
        return "http://" + getHost() + ":" + getMappedPort(METRICS_PORT) + "/metrics";
    }
}
