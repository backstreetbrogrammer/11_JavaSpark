package com.backstreetbrogrammer.chapter05_rddmapping;

import org.apache.commons.lang3.RandomStringUtils;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDMappingTest {

    private static final List<String> data = new ArrayList<>();
    private final int noOfIterations = 10;

    @BeforeAll
    static void beforeAll() {
        final var dataSize = 100_000;
        for (int i = 0; i < dataSize; i++) {
            data.add(RandomStringUtils.random(ThreadLocalRandom.current().nextInt(10)));
        }
        assertEquals(dataSize, data.size());
    }

    @Test
    @DisplayName("Test map operation using Spark RDD")
    void testMapOperationUsingSparkRDD() {
        final var conf = new SparkConf().setAppName("RDDMappingTest").setMaster("local[*]");
        final var sc = new JavaSparkContext(conf);
        final var myRdd = sc.parallelize(data);

        final Instant start = Instant.now();
        for (int i = 0; i < noOfIterations; i++) {
            final var strLengths = myRdd.map(String::length)
                                        .collect();
            System.out.println("[Spark RDD] List size:" + strLengths.size());
        }
        final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
        System.out.printf("[Spark RDD] time taken: %d ms%n%n", timeElapsed);

        sc.close();
    }

    @Test
    @DisplayName("Test map operation using Java Streams")
    void testMapOperationUsingJavaStreams() {
        final Instant start = Instant.now();
        for (int i = 0; i < noOfIterations; i++) {
            final var strLengths = data.stream()
                                       .map(String::length)
                                       .collect(Collectors.toList());
            System.out.println("[Java Streams] List size:" + strLengths.size());
        }
        final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
        System.out.printf("[Java Streams] time taken: %d ms%n%n", timeElapsed);
    }

    @Test
    @DisplayName("Test map operation using Java Parallel Streams")
    void testMapOperationUsingJavaParallelStreams() {
        final Instant start = Instant.now();
        for (int i = 0; i < noOfIterations; i++) {
            final var strLengths = data.parallelStream()
                                       .map(String::length)
                                       .collect(Collectors.toList());
            System.out.println("[Java Parallel Streams] List size:" + strLengths.size());
        }
        final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
        System.out.printf("[Java Parallel Streams] time taken: %d ms%n%n", timeElapsed);
    }
}
