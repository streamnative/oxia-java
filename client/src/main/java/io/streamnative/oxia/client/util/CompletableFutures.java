package io.streamnative.oxia.client.util;

import lombok.experimental.UtilityClass;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

@UtilityClass
public final class CompletableFutures {

    public static Throwable unwrap(Throwable ex) {
        final Throwable rc;
        if (ex instanceof CompletionException || ex instanceof ExecutionException) {
            rc = ex.getCause() != null ? ex.getCause() : ex;
        } else {
            rc = ex;
        }
        return rc;
    }

    public static CompletionException wrap(Throwable ex) {
        if (ex instanceof CompletionException) {
            return (CompletionException) ex;
        } else {
            return new CompletionException(ex);
        }
    }
}
