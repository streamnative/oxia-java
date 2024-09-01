/*
 * Copyright © 2022-2024 StreamNative Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
                                          AsyncLock.LockStatus actual) {
            super("illegal lock status. expect: " + expect.name() + ", actual: " + actual.name());
            this.expect = expect;
            this.actual = actual;
        }
    }

    @Getter
    public static final class UnknownLockStatusException extends LockException {
        private final AsyncLock.LockStatus actual;

        public UnknownLockStatusException(AsyncLock.LockStatus actual) {
            super("unknown lock status: " + actual.name());
            this.actual = actual;
        }
    }
}
