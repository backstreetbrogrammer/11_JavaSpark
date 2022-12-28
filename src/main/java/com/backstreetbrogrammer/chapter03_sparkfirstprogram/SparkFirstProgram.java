package com.backstreetbrogrammer.chapter03_sparkfirstprogram;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkFirstProgram {

    public static void main(final String[] args) {
        try (final var spark = SparkSession.builder()
                                           .appName("SparkFirstProgram")
                                           .master("local[*]")
                                           .getOrCreate();
             final var sc = new JavaSparkContext(spark.sparkContext())) {
            final var data
                    = Stream.iterate(1, n -> n + 1)
                            .limit(5)
                            .collect(Collectors.toList());

            final var myRdd = sc.parallelize(data);
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.printf("Default number of partitions: %d%n", myRdd.getNumPartitions());

            final var max = myRdd.reduce(Integer::max);
            final var min = myRdd.reduce(Integer::min);
            final var sum = myRdd.reduce(Integer::sum);
            System.out.printf("MAX~>%d, MIN~>%d, SUM~>%d%n", max, min, sum);

            try (final var scanner = new Scanner(System.in)) {
                scanner.nextLine();
            }
        }
    }
}
