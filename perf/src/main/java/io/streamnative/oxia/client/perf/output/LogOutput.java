package io.streamnative.oxia.client.perf.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class LogOutput implements Output {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void report(BenchmarkReport report) {
        final String s;
        try {
            s = MAPPER.writeValueAsString(report);
        } catch (Throwable ex) {
            throw new OutputException(ex.getMessage());
        }
        log.info(s);
    }
}
