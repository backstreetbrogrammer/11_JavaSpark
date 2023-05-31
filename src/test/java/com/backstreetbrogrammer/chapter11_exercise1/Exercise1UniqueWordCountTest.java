package com.backstreetbrogrammer.chapter11_exercise1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.List;

public class Exercise1UniqueWordCountTest {

    @Test
    @DisplayName("Exercise 1 - Unique Word Count from a file")
    void exercise1UniqueWordCount() {
        final var conf = new SparkConf().setAppName("Exercise1UniqueWordCountTest").setMaster("local[*]");
        try (final var sc = new JavaSparkContext(conf)) {
            final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
            final var filteredWords
                    = sc.textFile(testFilePath)
                        .map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                        .flatMap(line -> List.of(line.split("\\s")).iterator())
                        .filter(word -> ((word != null) && (word.trim().length() > 0)));

            final var counts
                    = filteredWords.mapToPair(word -> new Tuple2<>(word, 1L))
                                   .reduceByKey(Long::sum);

            counts.take(10).forEach(System.out::println);
            System.out.println("--------------------");

            // find top 10 words with maximum count
            System.out.println("Top 10 words with max count:");
            counts.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                  .sortByKey(false)
                  .take(10)
                  .forEach(tuple -> System.out.printf("(%s,%d)%n", tuple._2, tuple._1));

        }
    }

}
