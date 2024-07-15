package io.streamnative.oxia.client.perf.ycsb.generator;

public interface Generator<V> {

    V nextValue();
}
