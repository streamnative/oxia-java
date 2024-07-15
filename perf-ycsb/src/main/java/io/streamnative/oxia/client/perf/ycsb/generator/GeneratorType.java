package io.streamnative.oxia.client.perf.ycsb.generator;

import java.util.Locale;
import static java.util.Objects.requireNonNull;

public enum GeneratorType {
  UNIFORM,
  ZIPFIAN,
  SEQUENTIAL;


  public static GeneratorType fromString(String type) {
    requireNonNull(type);
    for (GeneratorType gType : GeneratorType.values()) {
      if (gType.name().toLowerCase(Locale.ROOT)
          .equals(type.toLowerCase(Locale.ROOT))) {
        return gType;
      }
    }
    throw new IllegalArgumentException("unknown generator type:" + type);
  }
}
