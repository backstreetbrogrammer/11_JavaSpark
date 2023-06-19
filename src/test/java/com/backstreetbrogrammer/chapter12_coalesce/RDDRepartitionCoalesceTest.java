package com.backstreetbrogrammer.chapter12_coalesce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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

public class RDDRepartitionCoalesceTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDRepartitionCoalesceTest")
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

    private void benchmarkTest(final JavaRDD<Double> myRdd, final String operation) {
        final var start = Instant.now();
        for (int i = 0; i < noOfIterations; i++) {
            final var sum = myRdd.reduce(Double::sum);
            System.out.printf("[Spark RDD %s] SUM: %f%n", operation, sum);
        }
        final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
        System.out.printf("[Spark RDD %s] time taken: %d ms for total partitions: %d%n%n",
                          operation, timeElapsed, myRdd.getNumPartitions());
        System.out.println("---------------------------------------");
    }

    @Test
    @DisplayName("Test repartition() in Spark RDD")
    void testRepartitionInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            System.out.printf("[Repartition] defaultMinPartitions=%d, defaultParallelism=%d%n%n",
                              sparkContext.defaultMinPartitions(),
                              sparkContext.defaultParallelism());

            System.out.println("---------------------------------------");

            final var myRdd = sparkContext.parallelize(data, 14);

            benchmarkTest(myRdd, "Repartition");
            benchmarkTest(myRdd.repartition(28), "Repartition");
            benchmarkTest(myRdd.repartition(7), "Repartition");
        }
    }

    @Test
    @DisplayName("Test coalesce() in Spark RDD")
    void testCoalesceInSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            System.out.printf("[Coalesce] defaultMinPartitions=%d, defaultParallelism=%d%n%n",
                              sparkContext.defaultMinPartitions(),
                              sparkContext.defaultParallelism());

            System.out.println("---------------------------------------");

            final var myRdd = sparkContext.parallelize(data, 14);

            benchmarkTest(myRdd, "Coalesce");
            benchmarkTest(myRdd.coalesce(28), "Coalesce"); // can only decrease partitions
            benchmarkTest(myRdd.coalesce(7), "Coalesce");
        }
    }
}
