package io.streamnative.oxia.client.perf.generator;

public interface Generator<V> {

    V nextValue();
}
