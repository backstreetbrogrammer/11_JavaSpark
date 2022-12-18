package com.backstreetbrogrammer.chapter03_sparkfirstprogram;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Scanner;

public class SparkFirstProgram {

    public static void main(final String[] args) {
        final var conf = new SparkConf().setAppName("SparkFirstProgram").setMaster("local[*]");

        try (final var sc = new JavaSparkContext(conf)) {
            final var data = List.of(165, 254, 124656, 356838, 64836);

            final var myRdd = sc.parallelize(data);
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
