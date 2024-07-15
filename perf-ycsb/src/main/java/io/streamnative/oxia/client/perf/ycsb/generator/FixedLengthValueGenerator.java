package io.streamnative.oxia.client.perf.ycsb.generator;

import java.util.concurrent.ThreadLocalRandom;

final class FixedLengthValueGenerator implements Generator<byte[]> {
  private final byte[] payload;

  public FixedLengthValueGenerator(int size) {
    payload = new byte[size];
    ThreadLocalRandom.current().nextBytes(payload);
  }

  @Override
  public byte[] nextValue() {
    return payload;
  }
}
