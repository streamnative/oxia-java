package io.streamnative.oxia.client.perf.generator;

import java.util.Objects;

import static java.util.Objects.*;

public final class Generators {

    public static Generator<String> createKeyGenerator(KeyGeneratorOptions options) {
        switch (options.type()) {
            case SEQUENTIAL -> new SequentialKeyGenerator(options);
            case UNIFORM -> new UniformKeyGenerator(options);
            case ZIPFIAN -> new ZipfianKeyGenerator(options);
        }
        throw new UnsupportedOperationException("unsupported yet");
    }

    public static Generator<OperationType> createOperationGenerator(OperationGeneratorOptions options) {
        requireNonNull(options);
        return new OperationGenerator(options);
    }

    public static Generator<>

}
