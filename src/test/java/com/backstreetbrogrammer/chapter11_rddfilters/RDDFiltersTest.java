package com.backstreetbrogrammer.chapter11_rddfilters;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

public class RDDFiltersTest {

    @Test
    @DisplayName("Test filter() method in Spark RDD")
    void testFilterInSparkRDD() {
        final var conf = new SparkConf().setAppName("RDDFiltersTest").setMaster("local[*]");
        try (final var sc = new JavaSparkContext(conf)) {

            final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
            final var lines = sc.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", lines.count());

            final var words = lines.flatMap(line -> List.of(line.split("\\s")).iterator());
            System.out.printf("Total number of words in the file~>%d%n", words.count());

            final var filteredWords = words.filter(word -> ((word != null) && (word.trim().length() > 0)));
            System.out.printf("Total number of words after filtering~>%d%n", filteredWords.count());

            System.out.println("First few words:");
            filteredWords.take(10).forEach(System.out::println);
            System.out.println("--------------------");
        }
    }

}
