package io.streamnative.oxia.client.api;

import java.util.function.Consumer;
import lombok.NonNull;

public interface ClientBuilder<B extends ClientBuilder> {
    @NonNull B notificationCallback(@NonNull Consumer<Notification> callback);
}
