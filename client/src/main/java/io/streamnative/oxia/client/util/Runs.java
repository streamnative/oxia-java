package io.streamnative.oxia.client.util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

@Slf4j
@UtilityClass
public final class Runs {

    public static void safeRun(Logger logger, Runnable runnable) {
        Objects.requireNonNull(logger);
        Objects.requireNonNull(runnable);
        try {
            runnable.run();
        } catch (Throwable ex) {
            logger.warn("exception when calling runnable ", ex);
        }
    }

    public static void safeExecute(Logger logger, ExecutorService executorService, Runnable runnable) {
        Objects.requireNonNull(logger);
        Objects.requireNonNull(executorService);
        Objects.requireNonNull(runnable);
        try {
            executorService.execute(runnable);
        } catch (RejectedExecutionException ex) {
            log.warn("Task executor rejected submission of task,"
                     + " using current thread do be backup. please give enough queue size for your executor.", ex);
            safeRun(logger, runnable);
        }
    }
}
