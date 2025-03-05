/*
 * Copyright © 2022-2024 StreamNative Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.oxia.client.api;

import lombok.NonNull;

public sealed interface GetOption
        permits OptionComparisonType, OptionIncludeValue, OptionPartitionKey {

    /** ComparisonEqual sets the Get() operation to compare the stored key for equality. */
    GetOption ComparisonEqual = new OptionComparisonType(OptionComparisonType.ComparisonType.Equal);

    /**
     * ComparisonFloor option will make the get operation to search for the record whose key is the
     * highest key <= to the supplied key.
     */
    GetOption ComparisonFloor = new OptionComparisonType(OptionComparisonType.ComparisonType.Floor);

    /**
     * ComparisonCeiling option will make the get operation to search for the record whose key is the
     * lowest key >= to the supplied key.
     */
    GetOption ComparisonCeiling =
            new OptionComparisonType(OptionComparisonType.ComparisonType.Ceiling);

    /**
     * ComparisonLower option will make the get operation to search for the record whose key is
     * strictly < to the supplied key.
     */
    GetOption ComparisonLower = new OptionComparisonType(OptionComparisonType.ComparisonType.Lower);

    /**
     * ComparisonHigher option will make the get operation to search for the record whose key is
     * strictly > to the supplied key.
     */
    GetOption ComparisonHigher = new OptionComparisonType(OptionComparisonType.ComparisonType.Higher);

    /**
     * PartitionKey overrides the partition routing with the specified `partitionKey` instead of the
     * regular record key.
     *
     * <p>Records with the same partitionKey will always be guaranteed to be co-located in the same
     * Oxia shard.
     *
     * @param partitionKey the partition key to use
     */
    static GetOption PartitionKey(@NonNull String partitionKey) {
        return new OptionPartitionKey(partitionKey);
    }

    /**
     * Creates and returns a GetOption that specifies whether to include a value. This method is used
     * to configure whether a value should be included in the operation.
     *
     * @param includeValue A boolean flag indicating whether the value should be included. - true: The
     *     value will be included. - false: The value will not be included.
     * @return A GetOption instance representing the include value setting.
     */
    static GetOption IncludeValue(boolean includeValue) {
        return new OptionIncludeValue(includeValue);
    }
}
