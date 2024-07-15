package io.streamnative.oxia.client.perf.ycsb;

import picocli.CommandLine;

public final class WorkerStarter {

  public static void main(String[] args) {
    int exitCode = new CommandLine(new OxiaOptions()).execute(args);
    System.exit(exitCode);
  }
}
