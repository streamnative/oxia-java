package io.streamnative.oxia.client.perf.ycsb.generator;


import java.util.concurrent.ThreadLocalRandom;

final class UniformKeyGenerator implements Generator<Long> {
    private final long bound;


    public UniformKeyGenerator(KeyGeneratorOptions options) {
        this.bound = options.bound();
    }

    @Override
    public Long nextValue() {
        if (bound == 0) {
            return Math.abs(ThreadLocalRandom.current().nextLong());
        }
        return Math.abs(ThreadLocalRandom.current().nextLong(bound));
    }
}
