package io.streamnative.oxia.client.perf.generator;

import java.util.concurrent.atomic.AtomicLong;

final class SequentialKeyGenerator implements Generator<String> {
    private final AtomicLong counter = new AtomicLong(0);
    private final String prefix;

    public SequentialKeyGenerator(KeyGeneratorOptions options) {
        this.prefix = options.prefix();
    }

    @Override
    public String nextValue() {
        final long nextNum = counter.getAndIncrement();
        if (prefix == null) {
            return nextNum + "";
        }
        return prefix + nextNum;
    }
}
