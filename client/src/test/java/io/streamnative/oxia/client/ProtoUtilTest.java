package io.streamnative.oxia.client;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ProtoUtilTest {

    @Test
    void uint32ToLong() {
        assertThat(ProtoUtil.uint32ToLong(1)).isEqualTo(1L);
        assertThat(ProtoUtil.uint32ToLong(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
        assertThat(ProtoUtil.uint32ToLong(Integer.MIN_VALUE)).isEqualTo(Integer.MAX_VALUE + 1L);
        assertThat(ProtoUtil.uint32ToLong(Integer.MIN_VALUE + 1)).isEqualTo(Integer.MAX_VALUE + 2L);
        assertThat(ProtoUtil.uint32ToLong(-1)).isEqualTo(4294967295L);
        assertThat(ProtoUtil.uint32ToLong(0)).isEqualTo(0L);
    }

    @Test
    void longToUnit32() {
        assertThat(ProtoUtil.longToUint32(1L)).isEqualTo(1);
        assertThat(ProtoUtil.longToUint32(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
        assertThat(ProtoUtil.longToUint32(Integer.MAX_VALUE + 1L)).isEqualTo(Integer.MIN_VALUE);
        assertThat(ProtoUtil.longToUint32(Integer.MAX_VALUE + 2L)).isEqualTo(Integer.MIN_VALUE + 1);
        assertThat(ProtoUtil.longToUint32(4294967295L)).isEqualTo(-1);
        assertThat(ProtoUtil.longToUint32(0L)).isEqualTo(0);
    }
}
