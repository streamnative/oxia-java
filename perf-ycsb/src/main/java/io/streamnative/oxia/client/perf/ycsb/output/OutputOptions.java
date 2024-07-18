package io.streamnative.oxia.client.perf.ycsb.output;

public record OutputOptions(
        /* log output */
        boolean pretty,
        /* pulsar output */
        PulsarOutputOptions pulsarOptions) {}
