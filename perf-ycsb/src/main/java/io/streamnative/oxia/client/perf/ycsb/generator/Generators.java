package io.streamnative.oxia.client.perf.ycsb.generator;


import static java.util.Objects.requireNonNull;

public final class Generators {

  public static Generator<String> createKeyGenerator(
      KeyGeneratorOptions options) {
    switch (options.type()) {
    case SEQUENTIAL -> new SequentialKeyGenerator(options);
    case UNIFORM -> new UniformKeyGenerator(options);
    case ZIPFIAN -> new ZipfianKeyGenerator(options);
    }
    throw new UnsupportedOperationException("unsupported yet");
  }

  public static Generator<OperationType> createOperationGenerator(
      OperationGeneratorOptions options) {
    requireNonNull(options);
    return new OperationGenerator(options);
  }


  public static Generator<byte[]> createFixedLengthValueGenerator(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException(
          "size can not lower than or equals to 0");
    }
    return new FixedLengthValueGenerator(size);
  }
}
