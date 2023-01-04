package com.backstreetbrogrammer.chapter14_rddshuffles;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDShuffleTest {

    @Test
    @DisplayName("Test shuffle operation in Spark RDD")
    void testShuffleOperationInSparkRDD() {
        final var conf = new SparkConf().setAppName("RDDShuffleTest").setMaster("local[*]");
        try (final var sc = new JavaSparkContext(conf)) {
            final String testFilePath = Path.of("src", "test", "resources", "wordslist.txt.gz").toString();

            final var initialRdd = sc.textFile(testFilePath);
            System.out.printf("Initial RDD Partition count: %d%n", initialRdd.getNumPartitions());

            final var repartitionedRdd = initialRdd.repartition(3); // shuffle
            System.out.printf("After repartition(), RDD Partition count: %d%n", repartitionedRdd.getNumPartitions());
            assertEquals(3, repartitionedRdd.getNumPartitions());

            final var countsRdd
                    = repartitionedRdd.mapToPair(word -> new Tuple2<>(word, 1L))
                                      .reduceByKey(Long::sum); // shuffle
            System.out.printf("After reduceByKey(), RDD Partition count: %d%n", countsRdd.getNumPartitions());

            // Though reduceByKey() triggers data shuffle, it doesn’t change the partition count as
            // RDD inherit the partition size from parent RDD.
            assertEquals(3, countsRdd.getNumPartitions());
        }
    }
}
