package io.streamnative.oxia.client.perf.generator;

public final class Generators {

    public static Generator<String> createKeyGenerator(KeyGeneratorOptions options) {
        switch (options.type()) {
            case SEQUENTIAL -> new SequentialKeyGenerator(options);
            case UNIFORM -> new UniformKeyGenerator(options);
            case ZIPFIAN -> new ZipfianKeyGenerator(options);

        }
    }

}
