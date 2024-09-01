package io.streamnative.oxia.client.api;

import java.util.concurrent.TimeUnit;

public record OptionAutoRevalidate(boolean enabled, long initDelay, long delay, TimeUnit unit) {

    public static final OptionAutoRevalidate DEFAULT = new OptionAutoRevalidate(true, 15, 15, TimeUnit.MINUTES);
}
