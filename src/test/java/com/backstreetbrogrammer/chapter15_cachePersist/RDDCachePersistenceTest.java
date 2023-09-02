package com.backstreetbrogrammer.chapter15_cachePersist;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDCachePersistenceTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDCachePersistenceTest").setMaster("local[*]");
    private final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
    private final int noOfIterations = 10;

    @Test
    @DisplayName("Test groupByKey() action without any cache() or persist()")
    void testGroupByKeyWithoutCacheOrPersist() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var lines = sparkContext.textFile(testFilePath);
                final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
                assertEquals(lines.count(), pairRDD.count());

                final var counts = pairRDD.groupByKey();
                counts.take(1).forEach(tuple -> {
                    final Integer integer = tuple._1;
                    final int size = Iterables.size(tuple._2);
                });
                pairRDD.count();
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
            System.out.println("---------------------------------------");
            System.out.printf("[GroupByKey] time taken: %d ms%n%n", timeElapsed);
        }
    }

    @Test
    @DisplayName("Test groupByKey() action with cache()")
    void testGroupByKeyWithCache() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var lines = sparkContext.textFile(testFilePath);
                final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
                assertEquals(lines.count(), pairRDD.count());

                final var cachedPairRdd = pairRDD.cache();
                final var counts = cachedPairRdd.groupByKey();
                counts.take(1).forEach(tuple -> {
                    final Integer integer = tuple._1;
                    final int size = Iterables.size(tuple._2);
                });
                cachedPairRdd.count();
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
            System.out.println("---------------------------------------");
            System.out.printf("[GroupByKey cached] time taken: %d ms%n%n", timeElapsed);
        }
    }

    @ParameterizedTest
    @MethodSource("getStorageLevel")
    @DisplayName("Test groupByKey() action with persist(StorageLevel)")
    void testGroupByKeyWithPersistMemory(final StorageLevel storageLevel, final String levelStr) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var lines = sparkContext.textFile(testFilePath);
                final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
                assertEquals(lines.count(), pairRDD.count());

                final var persistedPairRdd = pairRDD.persist(storageLevel);
                final var counts = persistedPairRdd.groupByKey();
                counts.take(1).forEach(tuple -> {
                    final Integer integer = tuple._1;
                    final int size = Iterables.size(tuple._2);
                });
                persistedPairRdd.count();
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIterations;
            System.out.println("---------------------------------------");
            System.out.printf("[GroupByKey Persisted-%s] time taken: %d ms%n%n", levelStr, timeElapsed);
        }
    }

    private static Stream<Arguments> getStorageLevel() {
        return Stream.of(
                Arguments.of(StorageLevel.MEMORY_ONLY(), "MEMORY_ONLY"),
                Arguments.of(StorageLevel.MEMORY_AND_DISK(), "MEMORY_AND_DISK"),
                Arguments.of(StorageLevel.MEMORY_ONLY_SER(), "MEMORY_ONLY_SER"),
                Arguments.of(StorageLevel.MEMORY_AND_DISK_SER(), "MEMORY_AND_DISK_SER"),
                Arguments.of(StorageLevel.DISK_ONLY(), "DISK_ONLY"),
                Arguments.of(StorageLevel.OFF_HEAP(), "OFF_HEAP"),
                Arguments.of(StorageLevel.NONE(), "NONE"),
                Arguments.of(StorageLevel.MEMORY_ONLY_2(), "MEMORY_ONLY_2"),
                Arguments.of(StorageLevel.MEMORY_AND_DISK_2(), "MEMORY_AND_DISK_2"),
                Arguments.of(StorageLevel.MEMORY_ONLY_SER_2(), "MEMORY_ONLY_SER_2"),
                Arguments.of(StorageLevel.MEMORY_AND_DISK_SER_2(), "MEMORY_AND_DISK_SER_2"),
                Arguments.of(StorageLevel.DISK_ONLY_2(), "DISK_ONLY_2"),
                Arguments.of(StorageLevel.DISK_ONLY_3(), "DISK_ONLY_3"));
    }
}
