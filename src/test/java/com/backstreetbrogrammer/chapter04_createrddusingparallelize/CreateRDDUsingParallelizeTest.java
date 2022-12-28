package com.backstreetbrogrammer.chapter04_createrddusingparallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateRDDUsingParallelizeTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("CreateRDDUsingParallelizeTest")
                                                       .setMaster("local[*]");

    @Test
    @DisplayName("Create an empty RDD with no partitions in Spark")
    void createAnEmptyRDDWithNoPartitionsInSpark() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var emptyRDD = sparkContext.emptyRDD();
            System.out.println(emptyRDD);
            System.out.printf("Default number of partitions: %d%n", emptyRDD.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create an empty RDD with default partitions in Spark")
    void createAnEmptyRDDWithDefaultPartitionsInSpark() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var emptyRDD = sparkContext.parallelize(List.of());
            System.out.println(emptyRDD);
            System.out.printf("Default number of partitions: %d%n", emptyRDD.getNumPartitions());
        }
    }

    @Test
    @DisplayName("Create a Spark RDD from Java collection using parallelize() method")
    void createSparkRDDUsingParallelizeMethod() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var data
                    = Stream.iterate(1, n -> n + 1)
                            .limit(8)
                            .collect(Collectors.toList());
            final var myRdd = sparkContext.parallelize(data);
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.printf("Default number of partitions: %d%n", myRdd.getNumPartitions());

            System.out.println("Elements of RDD: ");
            myRdd.collect().forEach(System.out::println);

            // reduce operations
            final var max = myRdd.reduce(Integer::max);
            final var min = myRdd.reduce(Integer::min);
            final var sum = myRdd.reduce(Integer::sum);
            System.out.printf("MAX~>%d, MIN~>%d, SUM~>%d%n", max, min, sum);
        }
    }

    @Test
    @DisplayName("Create a Spark RDD from Java collection using parallelize() method with given partitions")
    void createSparkRDDUsingParallelizeMethodWithGivenPartitions() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var data
                    = Stream.iterate(1, n -> n + 1)
                            .limit(8)
                            .collect(Collectors.toList());
            final var myRdd = sparkContext.parallelize(data, 8);
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.printf("Given number of partitions: %d%n", myRdd.getNumPartitions());

            System.out.println("Elements of RDD: ");
            myRdd.collect().forEach(System.out::println);

            // reduce operations
            final var max = myRdd.reduce(Integer::max);
            final var min = myRdd.reduce(Integer::min);
            final var sum = myRdd.reduce(Integer::sum);
            System.out.printf("MAX~>%d, MIN~>%d, SUM~>%d%n", max, min, sum);
        }
    }
}
