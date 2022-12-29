package com.backstreetbrogrammer.chapter09_rddtuples;

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

public class RDDTuplesTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDTuplesTest").setMaster("local[*]");

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test Tuples in Spark RDD")
    void testTuplesInSparkRDD(final String testFilePath) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", myRdd.count());

            final var tuple2JavaRDD = myRdd.map(line -> new Tuple2<>(line, line.length()));
            assertEquals(myRdd.count(), tuple2JavaRDD.count());

            tuple2JavaRDD.take(10).forEach(System.out::println);
            System.out.println("--------------------");
        }
    }

    private static Stream<Arguments> getFilePaths() {
        return Stream.of(
                Arguments.of(Path.of("src", "test", "resources", "1000words.txt").toString()),
                Arguments.of(Path.of("src", "test", "resources", "wordslist.txt.gz").toString()));
    }

}
