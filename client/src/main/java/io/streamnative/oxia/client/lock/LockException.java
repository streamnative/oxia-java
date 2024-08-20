package io.streamnative.oxia.client.lock;

import lombok.Getter;

public class LockException extends RuntimeException {
    public LockException(String message) {
        super(message);
    }


    @Getter
    public static class IllegalLockStatusException extends LockException {
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
