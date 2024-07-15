package io.streamnative.oxia.client.perf.ycsb.output;

public final class Outputs {


  public static Output createLogOutput() {
    return new LogOutput();
  }
}
