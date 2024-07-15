package io.streamnative.oxia.client.perf.ycsb.generator;

import java.util.concurrent.ThreadLocalRandom;

public class OperationGenerator implements Generator<OperationType> {
    private final OperationGeneratorOptions options;
    private final double writeThreshold;
    private final double readThreshold;
    private final double scanThreshold;


    public OperationGenerator(OperationGeneratorOptions options) {
        this.options = options;
        this.writeThreshold = options.writePercentage() * 100;
        this.readThreshold = (options.readPercentage() * 100) + writeThreshold;
        this.scanThreshold = (options.scanPercentage() * 100) + readThreshold;
    }

    @Override
    public OperationType nextValue() {
        final int rand = ThreadLocalRandom.current().nextInt(100);
        if (rand < writeThreshold) {
            return OperationType.WRITE;
        } else if (rand < readThreshold) {
            return OperationType.READ;
        } else if (rand < scanThreshold) {
            return OperationType.SCAN;
        }
        throw new IllegalStateException("out of bound: " + rand);
    }
}
