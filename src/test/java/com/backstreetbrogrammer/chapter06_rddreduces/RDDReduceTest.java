package com.backstreetbrogrammer.chapter06_rddreduces;

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

public class RDDReduceTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDReduceTest")
                                                       .setMaster("local[*]");

    private static final List<Double> data = new ArrayList<>();
    private final int noOfIterations = 10;

    @BeforeAll
    static void beforeAll() {
        final var dataSize = 1_000_000;
        for (int i = 0; i < dataSize; i++) {
            data.add(100 * ThreadLocalRandom.current().nextDouble() + 47);
        }
        assertEquals(dataSize, data.size());
    }

    @Test
    @DisplayName("Test reduce() action using Spark RDD")
    void testReduceActionUsingSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.parallelize(data, 14);

            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var sum = myRdd.reduce(Double::sum);
                System.out.println("[Spark RDD Reduce] SUM:" + sum);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD Reduce] time taken: %d ms%n%n", timeElapsed);
        }
    }

    @Test
    @DisplayName("Test fold() action using Spark RDD")
    void testFoldActionUsingSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.parallelize(data, 14);

            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var sum = myRdd.fold(0D, Double::sum);
                System.out.println("[Spark RDD Fold] SUM:" + sum);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD Fold] time taken: %d ms%n%n", timeElapsed);
        }
    }

    @Test
    @DisplayName("Test aggregate() action using Spark RDD")
    void testAggregateActionUsingSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.parallelize(data, 14);

            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var sum = myRdd.aggregate(0D, Double::sum, Double::sum);
                System.out.println("[Spark RDD Aggregate] SUM:" + sum);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
            System.out.printf("[Spark RDD Aggregate] time taken: %d ms%n%n", timeElapsed);
        }
    }

    @Test
    @DisplayName("Test reduce operation using Java Streams")
    void testReduceOperationUsingJavaStreams() {
        final Instant start = Instant.now();
        for (int i = 0; i < noOfIterations; i++) {
            final var sum = data.stream().reduce(Double::sum);
            System.out.println("[Java Streams] SUM:" + sum);
        }
        final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
        System.out.printf("[Java Streams] time taken: %d ms%n%n", timeElapsed);
    }

    @Test
    @DisplayName("Test reduce operation using Java Parallel Streams")
    void testReduceOperationUsingJavaParallelStreams() {
        final Instant start = Instant.now();
        for (int i = 0; i < noOfIterations; i++) {
            final var sum = data.parallelStream().reduce(Double::sum);
            System.out.println("[Java Parallel Streams] SUM:" + sum);
        }
        final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
        System.out.printf("[Java Parallel Streams] time taken: %d ms%n%n", timeElapsed);
    }
}
