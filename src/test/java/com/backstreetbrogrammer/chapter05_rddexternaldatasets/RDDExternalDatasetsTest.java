package com.backstreetbrogrammer.chapter05_rddexternaldatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class RDDExternalDatasetsTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDExternalDatasetsTest").setMaster("local[*]");

    @ParameterizedTest
    @ValueSource(strings = {
            "src\\test\\resources\\1000words.txt",
            "src\\test\\resources\\wordslist.txt.gz"
    })
    @DisplayName("Test loading local text file into Spark RDD")
    void testLoadingLocalTextFileIntoSparkRDDUsingValueSource(final String testFilePath) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.textFile(testFilePath);

            System.out.printf("Total lines in file %d%n", myRdd.count());
            System.out.println("Printing first 10 lines~>");

            myRdd.take(10).forEach(System.out::println);
            System.out.println("--------------------");
        }
    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("Test loading local text file into Spark RDD")
    void testLoadingLocalTextFileIntoSparkRDDUsingMethodSource(final String testFilePath) {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRdd = sparkContext.textFile(testFilePath);

            System.out.printf("Total lines in file %d%n", myRdd.count());
            System.out.println("Printing first 10 lines~>");

            myRdd.take(10).forEach(System.out::println);
            System.out.println("--------------------");
        }
    }

    @Test
    @DisplayName("Test loading whole directory into Spark RDD")
    void testLoadingWholeDirectoryIntoSparkRDDUsingMethodSource() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final String testDirPath = Path.of("src", "test", "resources").toString();
            final var myRdd = sparkContext.wholeTextFiles(testDirPath);

            System.out.printf("Total number of files in directory %s = %d%n", testDirPath, myRdd.count());

            myRdd.collect().forEach(tuple -> {
                System.out.printf("File name: %s%n", tuple._1);
                System.out.println("--------------------");
                if (tuple._1.endsWith("properties")) {
                    System.out.printf("Contents of %s : %n", tuple._1);
                    System.out.println(tuple._2);
                }
            });
        }
    }

    @Test
    @DisplayName("Test loading csv file into Spark RDD")
    void testLoadingCSVFileIntoSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final String testCSVFilePath = Path.of("src", "test", "resources", "dma.csv").toString();
            final var myRdd = sparkContext.textFile(testCSVFilePath);

            System.out.printf("Total lines in file %d%n", myRdd.count());

            System.out.println("CSV Headers~>");
            System.out.println(myRdd.first());
            System.out.println("--------------------");

            System.out.println("Printing first 10 lines~>");
            myRdd.take(10).forEach(System.out::println);
            System.out.println("--------------------");

            final var csvFields = myRdd.map(line -> line.split(","));
            csvFields.take(5)
                     .forEach(fields -> System.out.println(String.join("|", fields)));
        }
    }

    @Test
    @DisplayName("Test loading file from Amazon S3 into Spark RDD")
    void testLoadingAmazonS3FileIntoSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            // Replace Key with AWS account key (can find this on IAM)
            sparkContext.hadoopConfiguration().set("fs.s3a.access.key", "AWS access-key value");

            // Replace Key with AWS secret key (can find this on IAM)
            sparkContext.hadoopConfiguration().set("fs.s3a.secret.key", "AWS secret-key value");

            // Set the AWS S3 end point
            sparkContext.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");

            // Read a single zipped text file from S3 bucket
            final var myRdd = sparkContext.textFile(
                    "s3a://backstreetbrogrammer-bucket/1TrillionWords.txt.gz");

          /*System.out.printf("Total lines in file %d%n", myRdd.count());
            System.out.println("Printing first 10 lines~>");
            myRdd.take(10).forEach(System.out::println);*/

            assertThrows(AccessDeniedException.class, myRdd::count);
        }
    }

    private static Stream<Arguments> getFilePaths() {
        return Stream.of(
                Arguments.of(Path.of("src", "test", "resources", "1000words.txt").toString()),
                Arguments.of(Path.of("src", "test", "resources", "wordslist.txt.gz").toString()));
    }

}
