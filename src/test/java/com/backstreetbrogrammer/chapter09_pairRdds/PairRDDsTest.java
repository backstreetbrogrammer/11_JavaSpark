package com.backstreetbrogrammer.chapter09_pairRdds;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PairRDDsTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("PairRDDsTest").setMaster("local[*]");

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test mapToPair() method in Spark RDD")
    void testMapToPairInSparkRDD(final String testFilePath) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", myRdd.count());

            final var pairRDD = myRdd.mapToPair(line -> new Tuple2<>(line.length(), line));
            assertEquals(myRdd.count(), pairRDD.count());

            pairRDD.take(5).forEach(System.out::println);
            System.out.println("--------------------");
        }
    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test reduceByKey() method in Spark RDD")
    void testReduceByKeyInSparkRDD(final String testFilePath) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", lines.count());

            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            assertEquals(lines.count(), pairRDD.count());

            final var counts = pairRDD.reduceByKey(Long::sum);
            counts.take(5).forEach(tuple ->
                                           System.out.printf("Total strings of length %d are %d%n",
                                                             tuple._1, tuple._2));

            System.out.println("--------------------");
        }
    }


    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test groupByKey() method in Spark RDD")
    void testGroupByKeyInSparkRDD(final String testFilePath) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", lines.count());

            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            assertEquals(lines.count(), pairRDD.count());

            final var counts = pairRDD.groupByKey();
            counts.take(5).forEach(tuple ->
                                           System.out.printf("Total strings of length %d are %d%n",
                                                             tuple._1, Iterables.size(tuple._2)));

            System.out.println("--------------------");
        }
    }

    private static Stream<Arguments> getFilePaths() {
        return Stream.of(
                Arguments.of(Path.of("src", "test", "resources", "1000words.txt").toString()),
                Arguments.of(Path.of("src", "test", "resources", "wordslist.txt.gz").toString()));
    }

}
