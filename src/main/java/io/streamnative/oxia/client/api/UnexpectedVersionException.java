package io.streamnative.oxia.client.api;

import lombok.Getter;

public class UnexpectedVersionException extends OxiaException {
    @Getter private final long version;

    public UnexpectedVersionException(long version) {
        super("unexpected version: " + version);
        this.version = version;
    }
}