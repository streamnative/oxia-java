package io.streamnative.oxia.client.api;


import lombok.Getter;
import lombok.NonNull;

/** The key specified in the call did not exist on the server. */
public class KeyNotFoundException extends OxiaException {
    @Getter private final @NonNull String key;

    /**
     * Creates an instance of the exception.
     *
     * @param key The key specified in the call.
     */
    public KeyNotFoundException(@NonNull String key) {
        super("key not found: " + key);
        this.key = key;
    }
}
