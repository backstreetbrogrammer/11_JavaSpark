package com.backstreetbrogrammer.chapter12_closures;

import com.google.common.base.Strings;
import org.apache.spark.util.AccumulatorV2;

public class StringAccumulator extends AccumulatorV2<String, String> {
    private String strAccumulator = "";

    @Override
    public boolean isZero() {
        return strAccumulator.isEmpty();
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        final var stringAccumulator = new StringAccumulator();
        stringAccumulator.strAccumulator = this.strAccumulator;
        return stringAccumulator;
    }

    @Override
    public void reset() {
        strAccumulator = "";
    }

    @Override
    public void add(final String other) {
        if (!Strings.isNullOrEmpty(other)) {
            if (Strings.isNullOrEmpty(strAccumulator)) {
                strAccumulator = other.trim();
            } else {
                strAccumulator += ("," + other.trim());
            }
        }
    }

    @Override
    public void merge(final AccumulatorV2<String, String> other) {
        if (other instanceof StringAccumulator) {
            add(((StringAccumulator) other).strAccumulator);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Cannot merge %s with %s%n", this.getClass().getName(), other.getClass().getName()));
        }
    }

    @Override
    public String value() {
        return strAccumulator;
    }
}
