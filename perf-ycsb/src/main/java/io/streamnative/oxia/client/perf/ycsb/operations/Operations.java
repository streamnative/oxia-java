package io.streamnative.oxia.client.perf.ycsb.operations;

public interface Operations {

  Status read(String key);

  Status write(String key, byte[] value);

}
