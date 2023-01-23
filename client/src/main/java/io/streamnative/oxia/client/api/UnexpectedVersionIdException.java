package io.streamnative.oxia.client.api;


import lombok.Getter;

/** The versionId at the server did not that match supplied in the call. */
public class UnexpectedVersionIdException extends OxiaException {
    @Getter private final long versionId;

    /**
     * Creates an instance of the exception.
     *
     * @param versionId The record versionId to which the call was scoped.
     */
    public UnexpectedVersionIdException(long versionId) {
        super("unexpected versionId: " + versionId);
        this.versionId = versionId;
    }
}
