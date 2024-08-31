package io.streamnative.oxia.client.api.exceptions;

import io.streamnative.oxia.client.api.AsyncLock;
import lombok.Getter;

public sealed class LockException extends OxiaException {
    LockException(String message) {
        super(message);
    }

    LockException(Throwable cause) {
        super("", cause);
    }

    public static LockException wrap(Throwable ex) {
        if (ex instanceof LockException) {
            return (LockException) ex;
        } else {
            return new LockException(ex);
        }
    }

    public static final class LockBusyException extends LockException {
        public LockBusyException() {
            super("lock busy");
        }
    }

    public static final class AcquireTimeoutException extends LockException {
        public AcquireTimeoutException() {
            super("lock acquire timeout");
        }
    }


    @Getter
    public static final class IllegalLockStatusException extends LockException {
        private final AsyncLock.LockStatus expect;
        private final AsyncLock.LockStatus actual;

        public IllegalLockStatusException(AsyncLock.LockStatus expect,
                                          AsyncLock.LockStatus actual, String message) {
            super(message);
            this.expect = expect;
            this.actual = actual;
        }
    }
}
