package com.backstreetbrogrammer.chapter06_rddprinting;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RDDPrintingTest {

    private static final List<Double> data = new ArrayList<>();

    @BeforeAll
    static void beforeAll() {
        final var dataSize = 20;
        for (int i = 0; i < dataSize; i++) {
            data.add(100 * ThreadLocalRandom.current().nextDouble() + 47);
        }
        assertEquals(dataSize, data.size());
    }

    @Test
    @DisplayName("Test printing Spark RDD elements using foreach() only")
    void testPrintingSparkRDDElementsUsingOnlyForeach() {
        final var conf = new SparkConf().setAppName("RDDPrintingTest").setMaster("local[*]");
        final var sc = new JavaSparkContext(conf);
        final var myRdd = sc.parallelize(data);

        final Throwable exception = assertThrows(org.apache.spark.SparkException.class,
                                                 () -> myRdd.foreach(System.out::println));
        assertEquals(exception.getMessage(), "Task not serializable");

        sc.close();
    }


    @Test
    @DisplayName("Test printing Spark RDD elements using forEach() with collect() method")
    void testPrintingSparkRDDElementsUsingForeachWithCollect() {
        final var conf = new SparkConf().setAppName("RDDPrintingTest").setMaster("local[*]");
        final var sc = new JavaSparkContext(conf);
        final var myRdd = sc.parallelize(data);

        final Instant start = Instant.now();
        myRdd.collect().forEach(System.out::println);
        final long timeElapsed = Duration.between(start, Instant.now()).toMillis();
        System.out.printf("[Spark RDD] printing all - time taken: %d ms%n%n", timeElapsed);

        sc.close();
    }

    @Test
    @DisplayName("Test printing Spark RDD elements using forEach() with take() method")
    void testPrintingSparkRDDElementsUsingForeachWithTake() {
        final var conf = new SparkConf().setAppName("RDDPrintingTest").setMaster("local[*]");
        final var sc = new JavaSparkContext(conf);
        final var myRdd = sc.parallelize(data);

        final Instant start = Instant.now();
        myRdd.take(10).forEach(System.out::println);
        final long timeElapsed = Duration.between(start, Instant.now()).toMillis();
        System.out.printf("[Spark RDD] printing few - time taken: %d ms%n%n", timeElapsed);

        sc.close();
    }

}
