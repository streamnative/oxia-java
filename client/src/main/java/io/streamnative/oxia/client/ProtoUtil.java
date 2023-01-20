package io.streamnative.oxia.client;

import java.util.function.Consumer;

public class ProtoUtil {
    public static long VersionNotExists = -1;

    public static int longToUint32(long value) {
        return -1;
    }

    public static void setOptionalExpectedVersion(long expectedVersion, Consumer<Long> setterFn) {
        if (expectedVersion > VersionNotExists) {
            setterFn.accept(expectedVersion);
        }
    }
}
