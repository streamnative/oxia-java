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
package io.streamnative.oxia.client.util;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

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

    public static void safeExecute(
            Logger logger, ExecutorService executorService, Runnable runnable) {
        Objects.requireNonNull(logger);
        Objects.requireNonNull(executorService);
        Objects.requireNonNull(runnable);
        try {
            executorService.execute(runnable);
        } catch (RejectedExecutionException ex) {
            log.warn(
                    "Task executor rejected submission of task,"
                            + " using current thread do be backup. please give enough queue size for your executor.",
                    ex);
            safeRun(logger, runnable);
        }
    }
}
