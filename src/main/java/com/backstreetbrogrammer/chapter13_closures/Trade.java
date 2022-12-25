package com.backstreetbrogrammer.chapter13_closures;

import java.io.Serializable;
import java.util.Objects;

public class Trade implements Serializable {
    private static final long serialVersionUID = 47L;

    private String securityId;
    private long time;
    private double executedPrice;
    private int executedQuantity;
    private Side side;

    public Trade() {
    }

    public Trade(final String securityId,
                 final long time,
                 final double executedPrice,
                 final int executedQuantity,
                 final Side side) {
        this.securityId = securityId;
        this.time = time;
        this.executedPrice = executedPrice;
        this.executedQuantity = executedQuantity;
        this.side = side;
    }

    public String getSecurityId() {
        return securityId;
    }

    public void setSecurityId(final String securityId) {
        this.securityId = securityId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(final long time) {
        this.time = time;
    }

    public double getExecutedPrice() {
        return executedPrice;
    }

    public void setExecutedPrice(final double executedPrice) {
        this.executedPrice = executedPrice;
    }

    public int getExecutedQuantity() {
        return executedQuantity;
    }

    public void setExecutedQuantity(final int executedQuantity) {
        this.executedQuantity = executedQuantity;
    }

    public Side getSide() {
        return side;
    }

    public void setSide(final Side side) {
        this.side = side;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "securityId='" + securityId + '\'' +
                ", time=" + time +
                ", executedPrice=" + executedPrice +
                ", executedQuantity=" + executedQuantity +
                ", side=" + side +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Trade trade = (Trade) o;
        return time == trade.time
                && Double.compare(trade.executedPrice, executedPrice) == 0
                && executedQuantity == trade.executedQuantity
                && securityId.equals(trade.securityId)
                && side == trade.side;
    }

    @Override
    public int hashCode() {
        return Objects.hash(securityId, time, executedPrice, executedQuantity, side);
    }
}
