package com.github.skyrylyuk.sensoranalyzer.util;

import org.apache.commons.lang.math.NumberUtils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Project
 * Created by skyrylyuk on 2020-03-06.
 */
public class DoubleUtil {
    public static Double average(DoubleAdder adder, AtomicLong counter) throws Exception {
        final double sum = adder.sum();
        if (Double.isFinite(sum) && counter.longValue() > 0L) {
            return sum / counter.doubleValue();
        }
        return NumberUtils.DOUBLE_ZERO;
    }

    public static String formatDouble(double value) {
        return !NumberUtils.DOUBLE_ZERO.equals(value) ? String.format("%.02f", value) : null;
    }

}
