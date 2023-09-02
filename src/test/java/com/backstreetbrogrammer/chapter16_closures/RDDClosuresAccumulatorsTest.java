package com.backstreetbrogrammer.chapter16_closures;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class RDDClosuresAccumulatorsTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDClosuresAccumulatorsTest")
                                                       .setMaster("local[*]");

    @Test
    @DisplayName("Test closures in Java")
    void testJavaClosure() {
        String name = "John";
        // Runnable r = () -> System.out.println(name); // WILL NOT COMPILE
        name = "Betty";
        assertEquals("Betty", name);
    }

    @Test
    @DisplayName("Test incorrect way of using closure in Spark RDD")
    void testIncorrectWayOfUsingClosureInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var data = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            final var myRdd = sparkContext.parallelize(data);

            final long sum = 0;

            // myRdd.foreach(x -> sum += x); // WILL NOT COMPILE

            System.out.printf("Total sum: %d%n", sum);
        }
    }

    @Test
    @DisplayName("Test Long accumulator in Spark RDD")
    void testLongAccumulatorInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var accumulator = sparkContext.sc().longAccumulator("myLongAcc");
            assertTrue(accumulator.name().nonEmpty());
            assertEquals("myLongAcc", accumulator.name().get());

            final var data = List.of(1L, 2L, 3L, 4L, 5L);
            final var myRdd = sparkContext.parallelize(data);

            myRdd.foreach(accumulator::add);

            assertFalse(accumulator.isZero());

            assertEquals(15L, accumulator.sum());
            assertEquals(3D, accumulator.avg());
            assertEquals(5L, accumulator.count());

            // Only the driver program can read the accumulator’s value, using its value() method
            assertTrue(accumulator.isAtDriverSide());
            assertEquals(15L, accumulator.value());

            accumulator.reset();
            assertTrue(accumulator.isZero());
        }
    }

    @Test
    @DisplayName("Test Double accumulator in Spark RDD")
    void testDoubleAccumulatorInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var accumulator = sparkContext.sc().doubleAccumulator("myDoubleAcc");
            assertTrue(accumulator.name().nonEmpty());
            assertEquals("myDoubleAcc", accumulator.name().get());

            final var data = List.of(1D, 2D, 3D, 4D, 5D);
            final var myRdd = sparkContext.parallelize(data);

            myRdd.foreach(accumulator::add);

            assertFalse(accumulator.isZero());

            assertEquals(15D, accumulator.sum());
            assertEquals(3D, accumulator.avg());
            assertEquals(5L, accumulator.count());

            // Only the driver program can read the accumulator’s value, using its value() method
            assertTrue(accumulator.isAtDriverSide());
            assertEquals(15D, accumulator.value());

            accumulator.reset();
            assertTrue(accumulator.isZero());
        }
    }

    @Test
    @DisplayName("Test String accumulator in Spark RDD")
    void testStringAccumulatorInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var stringAccumulator = new StringAccumulator();
            sparkContext.sc().register(stringAccumulator, "stringAcc");
            assertTrue(stringAccumulator.name().nonEmpty());
            assertEquals("stringAcc", stringAccumulator.name().get());

            final var data
                    = List.of("Java", "Python", "JavaScript", "C++", "SQL");
            final var myRdd = sparkContext.parallelize(data);
            myRdd.foreach(stringAccumulator::add);

            assertFalse(stringAccumulator.isZero());

            // Only the driver program can read the accumulator’s value, using its value() method
            assertTrue(stringAccumulator.isAtDriverSide());
            assertEquals(new HashSet<>(data), Set.of(stringAccumulator.value().split(",")));

            stringAccumulator.reset();
            assertTrue(stringAccumulator.isZero());
        }
    }

    @Test
    @DisplayName("Test Stock Position accumulator in Spark RDD")
    void testStockPositionAccumulatorInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var securityId = "AAPL";
            final var applePositionAccumulator = new StockPositionAccumulator(securityId);
            sparkContext.sc().register(applePositionAccumulator, "applePositionAcc");
            assertTrue(applePositionAccumulator.name().nonEmpty());
            assertEquals("applePositionAcc", applePositionAccumulator.name().get());

            final var trade1 = new Trade("AAPL", 1000L, 50D, 100, Side.BUY);
            final var trade2 = new Trade("META", 2000L, 90D, 200, Side.SHORT_SELL);
            final var trade3 = new Trade("AAPL", 3000L, 51D, 100, Side.BUY);
            final var trade4 = new Trade("AAPL", 4000L, 52D, 200, Side.SELL);
            final var trade5 = new Trade("META", 5000L, 80D, 200, Side.BUY);
            final var trade6 = new Trade("TSLA", 6000L, 100D, 300, Side.BUY);

            final var trades1
                    = List.of(trade1, trade2, trade3);
            final var trades2
                    = List.of(trade4, trade5, trade6);

            final var stockPosition1 = new StockPosition(securityId);
            stockPosition1.addTrades(trades1);

            final var stockPosition2 = new StockPosition(securityId);
            stockPosition2.addTrades(trades2);

            final var appleStockPositions = List.of(stockPosition1, stockPosition2);

            final var myRdd = sparkContext.parallelize(appleStockPositions);
            myRdd.foreach(applePositionAccumulator::add);

            assertFalse(applePositionAccumulator.isZero());
            assertEquals(2L, applePositionAccumulator.count());

            // Only the driver program can read the accumulator’s value, using its value() method
            assertTrue(applePositionAccumulator.isAtDriverSide());
            final var accumulatorValue = applePositionAccumulator.value();
            assertNotNull(accumulatorValue);

            assertEquals(securityId, accumulatorValue.getSecurityId());
            assertEquals(3, accumulatorValue.getTrades().size());
            assertEquals(0D, accumulatorValue.getPosition());
            assertEquals(300D, accumulatorValue.getProfit());

            applePositionAccumulator.reset();
            assertTrue(applePositionAccumulator.isZero());
        }
    }

    @Test
    @DisplayName("Test Long accumulator lazy evaluation in Spark RDD")
    void testLongAccumulatorLazyEvaluationInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var accumulator = sparkContext.sc().longAccumulator("myLongAcc");
            assertTrue(accumulator.name().nonEmpty());
            assertEquals("myLongAcc", accumulator.name().get());

            final var data = List.of(1L, 2L, 3L, 4L, 5L);
            final var myRdd = sparkContext.parallelize(data);

            myRdd.map(x -> {
                accumulator.add(x);
                return x * x;
            });

            assertTrue(accumulator.isZero());
        }
    }

    /*@Test
    @DisplayName("Test custom accumulator in Spark RDD")
    void testCustomAccumulatorInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var positionAccumulator = new PositionAccumulator();
            sparkContext.sc().register(positionAccumulator, "myPositionAcc");
            assertTrue(positionAccumulator.name().nonEmpty());
            assertEquals("myPositionAcc", positionAccumulator.name().get());

            final var trade1 = new Trade("AAPL", 1000L, 50D, 100, Side.BUY);
            final var trade2 = new Trade("META", 2000L, 90D, 200, Side.SHORT_SELL);
            final var trade3 = new Trade("AAPL", 3000L, 51D, 100, Side.BUY);
            final var trade4 = new Trade("AAPL", 4000L, 52D, 200, Side.SELL);
            final var trade5 = new Trade("META", 5000L, 80D, 200, Side.BUY);
            final var trade6 = new Trade("TSLA", 6000L, 100D, 10, Side.BUY);

            final var trades
                    = List.of(trade1, trade2, trade3, trade4, trade5, trade6);

            final var myRdd = sparkContext.parallelize(trades);
            myRdd.foreach(positionAccumulator::add);

            assertFalse(positionAccumulator.isZero());
            assertEquals(6L, positionAccumulator.count());
            assertEquals(1300.0D, positionAccumulator.profit());

            positionAccumulator.getStockPosition()
                               .forEach((stock, position) ->
                                                System.out.printf("Stock %s has total %d position%n", stock,
                                                                  position));

            positionAccumulator.reset();
            assertTrue(positionAccumulator.isZero());
        }
    }*/

    @Test
    @DisplayName("Test merging of 2 maps in Java")
    void mergeTwoMaps() {
        final Map<String, Double> stockPosition1 = new HashMap<>() {{
            put("AAPL", 100D);
            put("META", 200D);
            put("TSLA", 100D);
        }};

        final Map<String, Double> stockPosition2 = new HashMap<>() {{
            put("AAPL", 100D);
            put("TSLA", 100D);
            put("GOOGL", 200D);
        }};

        stockPosition2.forEach((stock, position) -> stockPosition1.merge(stock, position, Double::sum));
        stockPosition1.forEach((stock, position) -> System.out.printf("Stock %s has total %f position%n", stock,
                                                                      position));
    }
}
