package io.streamnative.oxia.client.perf.ycsb;

public final class WorkerException extends RuntimeException {

  public WorkerException(String message) {
    super(message);
  }

  public WorkerException(Throwable cause) {
    super(cause);
  }
}
