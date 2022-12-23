package com.backstreetbrogrammer.chapter13_closures;

import org.apache.spark.util.AccumulatorV2;

public class PositionAccumulator extends AccumulatorV2<Trade, Trade> {
    private double position = 0D;
    private long count = 0L;

    @Override
    public boolean isZero() {
        return count == 0;
    }

    @Override
    public AccumulatorV2<Trade, Trade> copy() {
        final var positionAccumulator = new PositionAccumulator();
        positionAccumulator.position = this.position;
        positionAccumulator.count = this.count;
        return positionAccumulator;
    }

    @Override
    public void reset() {
        position = 0D;
        count = 0L;
    }

    @Override
    public void add(final Trade trade) {
        if (trade != null) {
            count += 1;
            if (trade.getSide() == Side.BUY) {
                position += (trade.getExecutedPrice() * trade.getExecutedQuantity());
            } else {
                position -= (trade.getExecutedPrice() * trade.getExecutedQuantity());
            }
        }
    }

    @Override
    public void merge(final AccumulatorV2<Trade, Trade> other) {
        if (other instanceof PositionAccumulator) {
            position += ((PositionAccumulator) other).position;
            count += ((PositionAccumulator) other).count;
        } else {
            throw new UnsupportedOperationException(
                    String.format("Cannot merge %s with %s%n", this.getClass().getName(), other.getClass().getName()));
        }
    }

    @Override
    public Trade value() {
        return null;
    }

    public double sum() {
        return position;
    }

    public long count() {
        return count;
    }
}
