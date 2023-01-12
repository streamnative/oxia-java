package io.streamnative.oxia.client.api;

public class KeyNotFoundException extends OxiaException {
    private final String key;

    public KeyNotFoundException(String key) {
        super("key not found: " + key);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}