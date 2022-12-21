package com.backstreetbrogrammer.chapter13_closures;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDClosuresAccumulatorsTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDClosuresAccumulatorsTest").setMaster("local[*]");

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
    @DisplayName("Test using accumulator for numeric type in Spark RDD")
    void testUsingAccumulatorForNumericTypeInSpark() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var accumulator = sparkContext.sc().longAccumulator();

            final var data = List.of(1, 2, 3, 4, 5);
            final var myRdd = sparkContext.parallelize(data);

            myRdd.foreach(accumulator::add);

            assertEquals(15, accumulator.sum());
            assertEquals(3D, accumulator.avg());
            assertEquals(5, accumulator.count());
            assertEquals(15, accumulator.value());
        }
    }
}
