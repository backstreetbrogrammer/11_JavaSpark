package com.backstreetbrogrammer.chapter13_closures;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.util.AccumulatorV2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Deprecated
public class PositionAccumulator extends AccumulatorV2<Trade, Trade> {
    private final Map<String, Integer> stockPosition = new ConcurrentHashMap<>();

    private long count = 0L;
    private double totalProfit = 0D;

    @Override
    public boolean isZero() {
        return count == 0 && stockPosition.isEmpty();
    }

    @Override
    public AccumulatorV2<Trade, Trade> copy() {
        final var positionAccumulator = new PositionAccumulator();
        positionAccumulator.count = this.count;
        positionAccumulator.totalProfit = this.totalProfit;
        positionAccumulator.stockPosition.putAll(this.stockPosition);
        return positionAccumulator;
    }

    @Override
    public void reset() {
        count = 0L;
        totalProfit = 0D;
        stockPosition.clear();
    }

    @Override
    public void add(final Trade trade) {
        if (trade != null) {
            count += 1;
            calculateStockPosition(trade);
            calculateTotalProfit(trade);
        }
    }

    private void calculateStockPosition(final Trade trade) {
        final var qty = trade.getExecutedQuantity();
        if (trade.getSide() == Side.BUY) {
            stockPosition.merge(trade.getSecurityId(), qty, Integer::sum);
        } else {
            stockPosition.merge(trade.getSecurityId(), qty * -1, (stock, position) -> position - qty);
        }
    }

    private void calculateTotalProfit(final Trade trade) {
        final var notional = trade.getExecutedPrice() * trade.getExecutedQuantity();
        if (trade.getSide() == Side.BUY) {
            totalProfit -= notional;
        } else {
            totalProfit += notional;
        }
    }

    @Override
    public void merge(final AccumulatorV2<Trade, Trade> other) {
        if (other instanceof PositionAccumulator) {
            count += ((PositionAccumulator) other).count;
            totalProfit += ((PositionAccumulator) other).totalProfit;
            ((PositionAccumulator) other).stockPosition
                    .forEach((stock, position) -> stockPosition.merge(stock, position, Integer::sum));
        } else {
            throw new UnsupportedOperationException(
                    String.format("Cannot merge %s with %s%n", this.getClass().getName(), other.getClass().getName()));
        }
    }

    @Override
    public Trade value() {
        // this is just dummy to avoid exceptions but not good for anything useful
        return new Trade();
    }

    public double profit() {
        return totalProfit;
    }

    public int position() {
        return stockPosition.values()
                            .stream()
                            .reduce(0, Integer::sum);
    }

    public long count() {
        return count;
    }

    public Map<String, Integer> getStockPosition() {
        return ImmutableMap.copyOf(stockPosition);
    }
}
