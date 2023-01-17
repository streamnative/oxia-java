package io.streamnative.oxia.client.api;


import java.util.function.Consumer;
import lombok.NonNull;

/**
 * A contract for Oxia client builders to expose notification handling in a consistent manner.
 *
 * @param <B> The Type of client builder
 */
public interface ClientBuilder<B extends ClientBuilder> {
    /**
     * Registers a callback to receive Oxia notification.
     *
     * @param callback The callback.
     * @return The builder instance, for fluency.
     */
    @NonNull
    B notificationCallback(@NonNull Consumer<Notification> callback);
}
