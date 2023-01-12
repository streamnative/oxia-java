package io.streamnative.oxia.client.api;


import lombok.Getter;

/** The version at the server did not that match supplied in the call. */
public class UnexpectedVersionException extends OxiaException {
    @Getter private final long version;

    /**
     * Creates an instance of the exception.
     *
     * @param version The record version to which the call was scoped.
     */
    public UnexpectedVersionException(long version) {
        super("unexpected version: " + version);
        this.version = version;
    }
}
