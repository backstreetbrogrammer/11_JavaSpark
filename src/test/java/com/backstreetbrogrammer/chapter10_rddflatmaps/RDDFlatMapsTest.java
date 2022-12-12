package com.backstreetbrogrammer.chapter10_rddflatmaps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

public class RDDFlatMapsTest {

    @Test
    @DisplayName("Test flatMap() method in Spark RDD")
    void testFlatMapInSparkRDD() {
        final var conf = new SparkConf().setAppName("RDDFlatMapsTest").setMaster("local[*]");
        final var sc = new JavaSparkContext(conf);

        final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
        final var lines = sc.textFile(testFilePath);
        System.out.printf("Total lines in file %d%n", lines.count());

        final var words = lines.flatMap(line -> List.of(line.split("\\s")).iterator());
        System.out.printf("Total number of words in the file~>%d%n", words.count());

        System.out.println("First few words:");
        words.take(10).forEach(System.out::println);
        System.out.println("--------------------");

        sc.close();
    }


}
