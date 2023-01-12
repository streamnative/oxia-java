package io.streamnative.oxia.client.api;

import static java.util.Collections.unmodifiableList;
import java.util.List;
import lombok.NonNull;

public record ListResult(@NonNull List<String> keys) {
    @NonNull public List<String> getKeys() {
        return unmodifiableList(keys);
    }
}

