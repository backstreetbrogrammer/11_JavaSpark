package com.backstreetbrogrammer.chapter13_closures;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

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

            long sum = 0;

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

    @Test
    @DisplayName("Test custom accumulator in Spark RDD")
    void testCustomAccumulatorInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var positionAccumulator = new PositionAccumulator();
            sparkContext.sc().register(positionAccumulator, "myPositionAcc");
            assertTrue(positionAccumulator.name().nonEmpty());
            assertEquals("myPositionAcc", positionAccumulator.name().get());

            final var trade1 = new Trade("AAPL", 1000L, 50.1D, 100, Side.BUY);
            final var trade2 = new Trade("META", 2000L, 94.8D, 200, Side.SHORT_SELL);
            final var trade3 = new Trade("AAPL", 3000L, 52.4D, 300, Side.BUY);
            final var trade4 = new Trade("AAPL", 4000L, 53.2D, 300, Side.SELL);
            final var trade5 = new Trade("META", 5000L, 93.6D, 200, Side.BUY);

            final var trades
                    = List.of(trade1, trade2, trade3, trade4, trade5);

            final var myRdd = sparkContext.parallelize(trades);
            myRdd.foreach(positionAccumulator::add); // spark.eventLog.enabled = false
            assertFalse(positionAccumulator.isZero());

            assertEquals(5L, positionAccumulator.count());
            assertEquals(4530.0D, positionAccumulator.sum());

            positionAccumulator.reset();
            assertTrue(positionAccumulator.isZero());
        }
    }
}
