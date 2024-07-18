package io.streamnative.oxia.client.perf.ycsb.output;

import static com.google.common.base.Strings.*;

public record PulsarOutputOptions(
        String serviceURL,
        String targetTopic,
        String authenticationPlugin,
        String authenticationParams) {

    public boolean validate() {
        if (isNullOrEmpty(serviceURL) || isNullOrEmpty(targetTopic)) {
            return false;
        }
        return isNullOrEmpty(authenticationPlugin) || !isNullOrEmpty(authenticationParams);
    }
}
