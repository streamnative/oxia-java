package io.streamnative.oxia.client.perf.ycsb;

import picocli.CommandLine;

@CommandLine.Command(name = "oxia", subcommands = {WorkerOptions.class})
public final class OxiaOptions implements Runnable {

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @Override
  public void run() {
    spec.helpCommand();
  }
}
