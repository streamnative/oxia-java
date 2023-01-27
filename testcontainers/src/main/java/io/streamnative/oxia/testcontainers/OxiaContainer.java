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

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class OxiaContainer extends GenericContainer<OxiaContainer> {

    private static final String DEFAULT_NETWORK_ALIAS = "localhost";
    public static final int OXIA_PORT = 6648;
    public static final int METRICS_PORT = 8080;

    public static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("streamnative/oxia:main");

    public OxiaContainer(DockerImageName imageName) {
        this(imageName, DEFAULT_NETWORK_ALIAS);
    }

    @SuppressWarnings("resource")
    public OxiaContainer(DockerImageName imageName, String networkAlias) {
        super(imageName);
        addExposedPorts(OXIA_PORT, METRICS_PORT);
        setCommand("oxia", "standalone", "--advertised-address", networkAlias);
        withNetworkAliases(networkAlias);
        waitingFor(
                Wait.forHttp("/metrics")
                        .forPort(METRICS_PORT)
                        .forStatusCode(200)
                        .forPath("/metrics")
                        .withStartupTimeout(Duration.of(30, SECONDS)));
    }

    public String getServiceAddress() {
        return getHost() + ":" + getMappedPort(OXIA_PORT);
    }

    public String getMetricsUrl() {
        return "http://" + getHost() + ":" + getMappedPort(METRICS_PORT) + "/metrics";
    }
}
