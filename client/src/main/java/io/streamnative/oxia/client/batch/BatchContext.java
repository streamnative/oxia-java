package io.streamnative.oxia.client.batch;


import java.time.Duration;
import lombok.NonNull;

record BatchContext(
        @NonNull Duration linger,
        int maxRequestsPerBatch,
        int batcherBufferSize,
        @NonNull Duration requestTimeout) {}
