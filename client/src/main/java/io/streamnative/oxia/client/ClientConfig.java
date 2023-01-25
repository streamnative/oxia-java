package io.streamnative.oxia.client;


import io.streamnative.oxia.client.api.Notification;
import java.time.Duration;
import java.util.function.Consumer;
import lombok.NonNull;

public record ClientConfig(
        @NonNull String serviceAddress,
        Consumer<Notification> notificationCallback,
        @NonNull Duration requestTimeout,
        @NonNull Duration batchLinger,
        int maxRequestsPerBatch,
        int operationQueueCapacity) {}
