package io.streamnative.oxia.client.options;

import io.streamnative.oxia.client.api.GetOption;
import io.streamnative.oxia.client.api.OptionComparisonType;
import io.streamnative.oxia.client.api.OptionIncludeValue;
import io.streamnative.oxia.client.api.OptionPartitionKey;
import io.streamnative.oxia.proto.KeyComparisonType;
import java.util.Set;

public record GetOptions(
        String partitionKey, boolean includeValue, KeyComparisonType comparisonType) {

    public static GetOptions parseFrom(Set<GetOption> options) {
        boolean includeValue = true;
        KeyComparisonType comparisonType = KeyComparisonType.EQUAL;
        String partitionKey = null;
        for (GetOption option : options) {
            if (option instanceof OptionIncludeValue) {
                includeValue = ((OptionIncludeValue) option).includeValue();
                continue;
            }
            if (option instanceof OptionComparisonType) {
                comparisonType =
                        switch (((OptionComparisonType) option).comparisonType()) {
                            case Floor -> KeyComparisonType.FLOOR;
                            case Lower -> KeyComparisonType.LOWER;
                            case Higher -> KeyComparisonType.HIGHER;
                            case Ceiling -> KeyComparisonType.CEILING;
                            default -> KeyComparisonType.EQUAL;
                        };
                continue;
            }
            if (option instanceof OptionPartitionKey) {
                partitionKey = ((OptionPartitionKey) option).partitionKey();
                continue;
            }
        }
        return new GetOptions(partitionKey, includeValue, comparisonType);
    }
}
