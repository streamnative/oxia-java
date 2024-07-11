package io.streamnative.oxia.client.perf.generator;

public record KeyGeneratorOptions(
        GeneratorType type,
        /* common parts  */
        String prefix,
        /* Uniform */
        long bound
        /* Zipfian */
        
) {

}
