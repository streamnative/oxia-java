package io.streamnative.oxia.client.util;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.OxiaClientBuilderImpl;
import io.streamnative.oxia.client.api.OxiaClientBuilder;
import lombok.experimental.UtilityClass;

import java.util.function.Consumer;

@UtilityClass
public final class ConfigUtils {

    public static ClientConfig getDefaultClientConfig() {
        final OxiaClientBuilderImpl builder = (OxiaClientBuilderImpl) OxiaClientBuilder.create("");
        return builder.getClientConfig();
    }

    public static ClientConfig getDefaultClientConfig(Consumer<OxiaClientBuilder> callback) {
        final OxiaClientBuilder builder = OxiaClientBuilder.create("");
        callback.accept(builder);
        return ((OxiaClientBuilderImpl) builder).getClientConfig();
    }
}
