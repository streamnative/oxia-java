package io.streamnative.oxia.client;


import java.nio.ByteBuffer;
import java.util.function.Consumer;
import lombok.NonNull;

public class ProtoUtil {
    public static final long VersionIdNotExists = -1;

    public static int longToUint32(long value) {
        return ByteBuffer.allocate(8).putLong(value).position(4).getInt();
    }

    public static long uint32ToLong(int unit32AsInt) {
        return ByteBuffer.allocate(8).putInt(0).putInt(unit32AsInt).flip().getLong();
    }

    public static void setOptionalExpectedVersionId(
            long expectedVersionId, @NonNull Consumer<Long> setterFn) {
        if (expectedVersionId > VersionIdNotExists) {
            setterFn.accept(expectedVersionId);
        }
    }
}
