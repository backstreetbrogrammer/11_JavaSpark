package com.backstreetbrogrammer.chapter13_closures;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StockPosition implements Serializable {
    private static final long serialVersionUID = 43L;

    private final String securityId;
    private final List<Trade> trades = new ArrayList<>();

    private double profit;
    private double position;

    public StockPosition(final String securityId) {
        this.securityId = securityId;
    }

    public String getSecurityId() {
        return securityId;
    }

    public double getProfit() {
        return profit;
    }

    public void setProfit(final double profit) {
        this.profit = profit;
    }

    public double getPosition() {
        return position;
    }

    public void setPosition(final double position) {
        this.position = position;
    }

    public List<Trade> getTrades() {
        return ImmutableList.copyOf(trades);
    }

    public void addTrades(final List<Trade> trades) {
        if (CollectionUtils.isNotEmpty(trades)) {
            final var filteredTrades = trades.stream()
                                             .filter(trade -> trade.getSecurityId().equals(securityId))
                                             .collect(Collectors.toList());
            this.trades.addAll(filteredTrades);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StockPosition that = (StockPosition) o;
        return Double.compare(that.profit, profit) == 0 && Double.compare(that.position, position) == 0 && securityId.equals(that.securityId) && trades.equals(that.trades);
    }

    @Override
    public int hashCode() {
        return Objects.hash(securityId, trades, profit, position);
    }
}
