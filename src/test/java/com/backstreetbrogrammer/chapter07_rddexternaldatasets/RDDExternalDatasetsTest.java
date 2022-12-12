package com.backstreetbrogrammer.chapter07_rddexternaldatasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class RDDExternalDatasetsTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "src\\test\\resources\\1000words.txt",
            "src\\test\\resources\\wordslist.txt.gz"
    })
    @DisplayName("Test loading local text file into Spark RDD")
    void testLoadingLocalTextFileIntoSparkRDD(final String testFilePath) {
        final var conf = new SparkConf().setAppName("RDDExternalDatasetsTest").setMaster("local[*]");
        final var sc = new JavaSparkContext(conf);

        // final var testFilePath = Path.of("src", "test", "resources", "1000words.txt").toString();
        final var myRdd = sc.textFile(testFilePath);

        System.out.printf("Total lines in file %d%n", myRdd.count());
        System.out.println("Printing first 10 lines~>");

        myRdd.take(10).forEach(System.out::println);
        System.out.println("--------------------");

        sc.close();
    }

}
