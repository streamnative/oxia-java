package io.streamnative.oxia.client.api;

public class UnexpectedVersionException extends OxiaException {
    private final long version;

    public UnexpectedVersionException(long version) {
        super("unexpected version: " + version);
        this.version = version;
    }

    public long getVersion() {
        return version;
    }
}