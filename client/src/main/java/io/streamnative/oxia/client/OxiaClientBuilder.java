package io.streamnative.oxia.client;


import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.ClientBuilder;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import java.time.Duration;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OxiaClientBuilder implements ClientBuilder<OxiaClientBuilder> {

    public static final Duration DefaultBatchLinger = Duration.ofMillis(5);
    public static final int DefaultMaxRequestsPerBatch = 1000;
    public static final Duration DefaultRequestTimeout = Duration.ofSeconds(30);
    public static final int DefaultOperationQueueCapacity = 1000;

    @NonNull private final String serviceAddress;
    private Consumer<Notification> notificationCallback;
    @NonNull private Duration requestTimeout = DefaultRequestTimeout;
    @NonNull private Duration batchLinger = DefaultBatchLinger;
    private int maxRequestsPerBatch = DefaultMaxRequestsPerBatch;
    private int operationQueueCapacity = DefaultOperationQueueCapacity;

    @Override
    public @NonNull OxiaClientBuilder notificationCallback(
            @NonNull Consumer<Notification> notificationCallback) {
        this.notificationCallback = notificationCallback;
        return this;
    }

    public @NonNull OxiaClientBuilder requestTimeout(@NonNull Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public @NonNull OxiaClientBuilder batchLinger(@NonNull Duration batchLinger) {
        this.batchLinger = batchLinger;
        return this;
    }

    public @NonNull OxiaClientBuilder maxRequestsPerBatch(int maxRequestsPerBatch) {
        if (maxRequestsPerBatch < 0) {
            throw new IllegalArgumentException(
                    "MaxRequestsPerBatch must be greater than zero: " + maxRequestsPerBatch);
        }
        this.maxRequestsPerBatch = maxRequestsPerBatch;
        return this;
    }

    public @NonNull OxiaClientBuilder operationQueueCapacity(int operationQueueCapacity) {
        if (operationQueueCapacity < 0) {
            throw new IllegalArgumentException(
                    "operationQueueCapacity must be greater than zero: " + operationQueueCapacity);
        }
        this.operationQueueCapacity = operationQueueCapacity;
        return this;
    }

    public @NonNull AsyncOxiaClient asyncClient() {
        return new AsyncOxiaClientImpl(
                null, // TODO
                null, // TODO
                null // TODO
                //                new ClientConfig(
                //                        serviceAddress,
                //                        notificationCallback,
                //                        requestTimeout,
                //                        batchLinger,
                //                        maxRequestsPerBatch,
                //                        operationQueueCapacity)
                );
    }

    public @NonNull SyncOxiaClient syncClient() {
        return new SyncOxiaClientImpl(asyncClient());
    }
}
