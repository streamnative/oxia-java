package io.streamnative.oxia.client.api;

import lombok.Getter;

public class KeyNotFoundException extends OxiaException {
    @Getter private final String key;

    public KeyNotFoundException(String key) {
        super("key not found: " + key);
        this.key = key;
    }
}