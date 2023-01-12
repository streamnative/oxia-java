package io.streamnative.oxia.client.api;

/** A super-class of exceptions describing errors that occured on an Oxia server. */
public abstract class OxiaException extends Exception {
    OxiaException(String message) {
        super(message);
    }
}
