package com.backstreetbrogrammer.chapter16_closures;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.*;


public class RDDBroadcastTest implements Serializable {

    private static final long serialVersionUID = 43L;

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDBroadcastTest")
                                                       .setMaster("local[*]");

    @Test
    @DisplayName("Test Broadcast variables in Spark RDD")
    void testBroadcastInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final List<String> tickers = List.of("AAPL", "META", "TSLA", "GOOGL");
            final Map<String, String> tickerStockName = new HashMap<>() {{
                put("AAPL", "Apple Inc");
                put("META", "Meta Platforms Inc");
                put("TSLA", "Tesla Inc");
                put("GOOGL", "Alphabet Inc");
            }};

            final Map<String, Double> tickerLastClosePrice = new HashMap<>() {{
                put("GOOGL", 200.5D);
                put("AAPL", 100.1D);
                put("META", 300.3D);
                put("TSLA", 180.7D);
            }};

            final Broadcast<Map<String, String>> broadcastTickerStockName
                    = sparkContext.broadcast(tickerStockName);
            final Broadcast<Map<String, Double>> broadcastTickerLastClosePrice =
                    sparkContext.broadcast(tickerLastClosePrice);
            try {
                final var myRdd = sparkContext.parallelize(tickers);
                final var lastClosePriceRdd = myRdd.map(ticker -> {
                    final var tickerFullName = broadcastTickerStockName.value().get(ticker);
                    final var tickerClosePrice = broadcastTickerLastClosePrice.value().get(ticker);
                    return String.format("Ticker=%s, Full Stock Name=%s, Last Close Price=%.2f",
                                         ticker,
                                         tickerFullName,
                                         tickerClosePrice);
                });

                lastClosePriceRdd.collect().forEach(System.out::println);

            } finally {
                /*
                broadcastTickerStockName.unpersist();
                broadcastTickerStockName.unpersist(true);
                broadcastTickerStockName.destroy();
                broadcastTickerStockName.destroy(true);
                */
                broadcastTickerStockName.destroy(true);
                broadcastTickerLastClosePrice.destroy(true);
            }
        }
    }

}
