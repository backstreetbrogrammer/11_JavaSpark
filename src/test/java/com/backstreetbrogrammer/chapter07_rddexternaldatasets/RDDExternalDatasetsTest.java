package com.backstreetbrogrammer.chapter07_rddexternaldatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.stream.Stream;

public class RDDExternalDatasetsTest {

    private JavaSparkContext sparkContext;

    @BeforeEach
    void setUp() {
        final var sparkConf = new SparkConf().setAppName("RDDExternalDatasetsTest").setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    @AfterEach
    void tearDown() {
        sparkContext.close();
    }


    @ParameterizedTest
    @ValueSource(strings = {
            "src\\test\\resources\\1000words.txt",
            "src\\test\\resources\\wordslist.txt.gz"
    })
    @DisplayName("Test loading local text file into Spark RDD")
    void testLoadingLocalTextFileIntoSparkRDDUsingValueSource(final String testFilePath) {
        final var myRdd = sparkContext.textFile(testFilePath);

        System.out.printf("Total lines in file %d%n", myRdd.count());
        System.out.println("Printing first 10 lines~>");

        myRdd.take(10).forEach(System.out::println);
        System.out.println("--------------------");
    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test loading local text file into Spark RDD")
    void testLoadingLocalTextFileIntoSparkRDDUsingMethodSource(final String testFilePath) {
        final var myRdd = sparkContext.textFile(testFilePath);

        System.out.printf("Total lines in file %d%n", myRdd.count());
        System.out.println("Printing first 10 lines~>");

        myRdd.take(10).forEach(System.out::println);
        System.out.println("--------------------");
    }

    private static Stream<Arguments> getFilePaths() {
        return Stream.of(
                Arguments.of(Path.of("src", "test", "resources", "1000words.txt").toString()),
                Arguments.of(Path.of("src", "test", "resources", "wordslist.txt.gz").toString()));
    }

}
