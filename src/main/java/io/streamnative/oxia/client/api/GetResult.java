package io.streamnative.oxia.client.api;

import lombok.NonNull;

public record GetResult(@NonNull byte[] payload, @NonNull Stat stat) {
}
