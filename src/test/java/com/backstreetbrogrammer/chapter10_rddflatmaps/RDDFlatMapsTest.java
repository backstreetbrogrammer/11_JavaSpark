package com.backstreetbrogrammer.chapter10_rddflatmaps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDFlatMapsTest {

    @Test
    @DisplayName("Test flatMap() method in Spark RDD")
    void testFlatMapInSparkRDD() {
        final var conf = new SparkConf().setAppName("RDDFlatMapsTest").setMaster("local[*]");
        try (final var sc = new JavaSparkContext(conf)) {

            final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
            final var lines = sc.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", lines.count());

            final var words = lines.flatMap(line -> List.of(line.split("\\s")).iterator());
            System.out.printf("Total number of words in the file~>%d%n", words.count());

            System.out.println("First few words:");
            words.take(10).forEach(System.out::println);
            System.out.println("--------------------");
        }
    }


    @Test
    @DisplayName("Test flatMap() method in Java Streams")
    void testFlatMapInJavaStreams() {
        final var students = List.of("John", "Mary", "Peter");
        final var favoriteLanguages = List.of("Java", "Python");

         /*
         Task: return pair of both the lists
         For ex:
         [("John", "Java"), ("John", "Python"), ("Mary", "Java"), ("Mary", "Python"), ("Peter", "Java"), ("Peter", "Python")]

         Solution:
         We could use two maps to iterate on the two lists and generate the pairs.
         But this would return a Stream<Stream<String[]>>.
         What we need to do is `flatten` the generated streams to result in a Stream<String[]>.
         */

        final var pairs
                = students.stream()
                          .flatMap(student -> favoriteLanguages.stream()
                                                               .map(favoriteLanguage ->
                                                                            new String[]{student, favoriteLanguage}))
                          .collect(Collectors.toList());
        assertEquals(6, pairs.size());

        pairs.forEach(val -> System.out.printf("(%s,%s)%n", val[0], val[1]));
    }

}
