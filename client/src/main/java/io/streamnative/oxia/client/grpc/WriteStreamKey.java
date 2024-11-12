package io.streamnative.oxia.client.grpc;

public record WriteStreamKey(long shard, int mod) {}
