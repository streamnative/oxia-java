package io.streamnative.oxia.client.util;

import lombok.experimental.UtilityClass;
import org.slf4j.Logger;

@UtilityClass
public final class Runs {

    public static void safeRun(Logger logger, Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable ex) {
            logger.warn("exception when calling runnable ", ex);
        }
    }
}
