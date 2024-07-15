package io.streamnative.oxia.client.perf.ycsb.operations;

public final class Status {
  private int code;
  private byte[] payload;
  private String errorInfo;

  private Status() {
  }

  public static Status success() {
    final Status status = new Status();
    status.code = CODE_SUCCESS;
    return status;
  }

  public static Status success(byte[] payload) {
    final Status status = new Status();
    status.code = CODE_SUCCESS;
    status.payload = payload;
    return status;
  }

  public boolean isSuccess() {
    return code == CODE_SUCCESS;
  }

  public static Status failed(String errorInfo) {
    final Status status = new Status();
    status.code = CODE_UNKNOWN;
    status.errorInfo = errorInfo;
    return status;
  }

  public static final int CODE_SUCCESS = 0;
  public static final int CODE_UNKNOWN = 1;

}
