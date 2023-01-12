package io.streamnative.oxia.client;


import io.streamnative.oxia.client.api.Notification;
import java.time.Duration;
import java.util.function.Consumer;
import lombok.NonNull;

record ClientConfig(
        @NonNull String serviceAddress,
        Consumer<Notification> notificationCallback,
        @NonNull Duration requestTimeout,
        @NonNull Duration batchLinger,
        @NonNull int maxRequestsPerBatch) {}
