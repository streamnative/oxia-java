package io.streamnative.oxia.client.perf.ycsb.output;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class Doubles {


    public static double format2Scale(double value) {
        BigDecimal decimal = new BigDecimal(value);
        decimal = decimal.setScale(2, RoundingMode.UP);
        return decimal.doubleValue();
    }
}
