package io.streamnative.oxia.client.perf.ycsb.generator;

public record OperationGeneratorOptions(
        double writePercentage,
        double readPercentage,
        double scanPercentage
        ) {

    public boolean validate() {
        return writePercentage + scanPercentage + readPercentage == 1.0;
    }
}
