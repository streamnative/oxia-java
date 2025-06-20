/*
 * Copyright © 2022-2025 StreamNative Inc.
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
package io.oxia.client.api;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.UnsupportedAuthenticationException;
import io.oxia.client.internal.DefaultImplementation;
import java.io.File;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface OxiaClientBuilder {

    static OxiaClientBuilder create(String serviceAddress) {
        return DefaultImplementation.getDefaultImplementation(serviceAddress);
    }

    SyncOxiaClient syncClient() throws OxiaException;

    CompletableFuture<AsyncOxiaClient> asyncClient();

    OxiaClientBuilder requestTimeout(Duration requestTimeout);

    OxiaClientBuilder batchLinger(Duration batchLinger);

    OxiaClientBuilder maxRequestsPerBatch(int maxRequestsPerBatch);

    OxiaClientBuilder namespace(String namespace);

    OxiaClientBuilder sessionTimeout(Duration sessionTimeout);

    OxiaClientBuilder clientIdentifier(String clientIdentifier);

    OxiaClientBuilder clientIdentifier(Supplier<String> clientIdentifier);

    OxiaClientBuilder openTelemetry(OpenTelemetry openTelemetry);

    OxiaClientBuilder authentication(Authentication authentication);

    OxiaClientBuilder connectionBackoff(Duration minDelay, Duration maxDelay);

    OxiaClientBuilder maxConnectionPerNode(int connections);

    OxiaClientBuilder connectionKeepAliveTimeout(Duration connectionKeepAliveTimeout);

    OxiaClientBuilder connectionKeepAliveTime(Duration connectionKeepAlive);

    /**
     * Configure the authentication plugin and its parameters.
     *
     * @param authPluginClassName the class name of the authentication plugin
     * @param authParamsString the parameters of the authentication plugin
     * @return the OxiaClientBuilder instance
     * @throws UnsupportedAuthenticationException if the authentication plugin is not supported
     */
    OxiaClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException;

    OxiaClientBuilder enableTls(boolean enableTls);

    /**
     * Load the configuration from the specified configuration file.
     *
     * @param configPath the path of the configuration file
     * @return the OxiaClientBuilder instance
     */
    OxiaClientBuilder loadConfig(String configPath);

    /**
     * Load the configuration from the specified configuration file.
     *
     * @param configFile the configuration file
     * @return the OxiaClientBuilder instance
     */
    OxiaClientBuilder loadConfig(File configFile);

    /**
     * Load the configuration from the specified properties.
     *
     * @param properties the properties
     * @return the OxiaClientBuilder instance
     */
    OxiaClientBuilder loadConfig(Properties properties);
}
