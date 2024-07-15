package io.streamnative.oxia.client.perf.ycsb;

import io.streamnative.oxia.client.OxiaClientBuilderImpl;
import picocli.CommandLine;

@CommandLine.Command(name = "ycsb")
public final class WorkerOptions implements Runnable {

  @CommandLine.Option(
      names = {"-a", "--service-addr"},
      description = "Oxia Service Address")
  String serviceAddr = "localhost:6648";

  @CommandLine.Option(
      names = {"-n", "--namespace"},
      description = "Oxia namespace")
  String namespace = "default";

  @CommandLine.Option(
      names = {"-r", "--rate"},
      description = "Request rate, ops/s")
  double requestsRate = 100.0;

  @CommandLine.Option(
      names = {"--batch-linger-ms"},
      description = "Batch linger time")
  long batchLingerMs = OxiaClientBuilderImpl.DefaultBatchLinger.toMillis();

  @CommandLine.Option(
      names = {"--max-requests-per-batch"},
      description = "Maximum requests per batch")
  int maxRequestsPerBatch = OxiaClientBuilderImpl.DefaultMaxRequestsPerBatch;

  @CommandLine.Option(
      names = {"--request-timeout-ms"},
      description = "Requests timeout")
  long requestTimeoutMs =
      OxiaClientBuilderImpl.DefaultRequestTimeout.toMillis();

  @CommandLine.Option(
      names = {"-o", "--max-outstanding-requests"},
      description = "Max number of outstanding requests to server")
  int maxOutstandingRequests = 100_000;


  /* Operations */
  @CommandLine.Option(
      names = {"--write-percent"},
      description = "Percentage of write requests, compared to total requests")
  double writePercentage = 20.0;

  @CommandLine.Option(
      names = {"--scan-percent"},
      description = "Percentage of scan requests, compared to total requests")
  double scanPercentage = 0.0;

  @CommandLine.Option(
      names = {"-p", "--read-write-percent"},
      description = "Percentage of read requests, compared to total requests")
  double readPercentage = 80.0;

  @CommandLine.Option(
      names = {"-num", "--operation-num"},
      description = "Num of total operations. 0 means no limit")
  long operationNum = 0;


  /* Generation */

  @CommandLine.Option(
      names = {"--key-distribution"},
      description = "The key distribution. support lcg,uniform,zipfian,sequence")
  String keyDistribution = "lcg";

  @CommandLine.Option(
      names = {"--key-prefix"},
      description = "The key prefix.")
  String keyPrefix = "key-";


  /* Uniform distribution */
  @CommandLine.Option(
      names = {"--key-uniform-bound"},
      description = "The upper bound of uniform key distribution. the default is 0 which means no bound")
  int bound = 0;



  /* Value Generation */

  @CommandLine.Option(
      names = {"-s", "--value-size"},
      description = "Size of the values to write")
  int valueSize = 128;


  @Override
  public void run() {
    try (Worker worker = new Worker(this)) {
      worker.run();
    }
  }
}
