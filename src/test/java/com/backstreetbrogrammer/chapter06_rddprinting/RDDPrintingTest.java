package com.backstreetbrogrammer.chapter06_rddprinting;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RDDPrintingTest {

    private static final List<Double> data = new ArrayList<>();
    private JavaSparkContext sparkContext;

    @BeforeAll
    static void beforeAll() {
        final var dataSize = 20;
        for (int i = 0; i < dataSize; i++) {
            data.add(100 * ThreadLocalRandom.current().nextDouble() + 47);
        }
        assertEquals(dataSize, data.size());
    }

    @BeforeEach
    void setUp() {
        final var sparkConf = new SparkConf().setAppName("RDDPrintingTest").setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    @AfterEach
    void tearDown() {
        sparkContext.close();
    }

    @Test
    @DisplayName("Test printing Spark RDD elements using foreach() only")
    void testPrintingSparkRDDElementsUsingOnlyForeach() {
        final var myRdd = sparkContext.parallelize(data);

        final Throwable exception = assertThrows(org.apache.spark.SparkException.class,
                                                 () -> myRdd.foreach(System.out::println));
        assertEquals(exception.getMessage(), "Task not serializable");
    }


    @Test
    @DisplayName("Test printing Spark RDD elements using forEach() with collect() method")
    void testPrintingSparkRDDElementsUsingForeachWithCollect() {
        final var myRdd = sparkContext.parallelize(data);

        final Instant start = Instant.now();
        myRdd.collect().forEach(System.out::println);
        final long timeElapsed = Duration.between(start, Instant.now()).toMillis();
        System.out.printf("[Spark RDD] printing all - time taken: %d ms%n%n", timeElapsed);
    }

    @Test
    @DisplayName("Test printing Spark RDD elements using forEach() with take() method")
    void testPrintingSparkRDDElementsUsingForeachWithTake() {
        final var myRdd = sparkContext.parallelize(data);

        final Instant start = Instant.now();
        myRdd.take(10).forEach(System.out::println);
        final long timeElapsed = Duration.between(start, Instant.now()).toMillis();
        System.out.printf("[Spark RDD] printing few - time taken: %d ms%n%n", timeElapsed);
    }

}
