package io.streamnative.oxia.client.api;


import lombok.Getter;

/** The versionId at the server did not that match supplied in the call. */
public class UnexpectedVersionIdException extends OxiaException {
    @Getter private final long version;

    /**
     * Creates an instance of the exception.
     *
     * @param version The record versionId to which the call was scoped.
     */
    public UnexpectedVersionIdException(long version) {
        super("unexpected versionId: " + version);
        this.version = version;
    }
}
