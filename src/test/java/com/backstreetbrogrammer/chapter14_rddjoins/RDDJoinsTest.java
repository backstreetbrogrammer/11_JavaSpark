package com.backstreetbrogrammer.chapter14_rddjoins;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RDDJoinsTest {

    private static final List<Tuple2<Integer, String>> customers = new ArrayList<>();
    private static final List<Tuple2<Integer, Double>> bills = new ArrayList<>();

    private JavaSparkContext sparkContext;
    private JavaPairRDD<Integer, String> customersPairs;
    private JavaPairRDD<Integer, Double> billsPairs;

    @BeforeAll
    static void beforeAll() {
        customers.add(new Tuple2<>(1, "John"));
        customers.add(new Tuple2<>(2, "Mary"));
        customers.add(new Tuple2<>(3, "Peter"));
        customers.add(new Tuple2<>(4, "Betty"));
        customers.add(new Tuple2<>(5, "David"));

        bills.add(new Tuple2<>(8, 150.5D));
        bills.add(new Tuple2<>(2, 199.9D));
        bills.add(new Tuple2<>(9, 200D));
        bills.add(new Tuple2<>(6, 300D));
        bills.add(new Tuple2<>(5, 400D));
        bills.add(new Tuple2<>(10, 250.9D));
        bills.add(new Tuple2<>(7, 123.9D));
    }

    @BeforeEach
    void setUp() {
        final var sparkConf = new SparkConf().setAppName("RDDJoinsTest").setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);

        customersPairs = sparkContext.parallelizePairs(customers);
        billsPairs = sparkContext.parallelizePairs(bills);
    }

    @AfterEach
    void tearDown() {
        sparkContext.close();
    }

    @Test
    @DisplayName("Test Inner Join in Spark RDD")
    void testInnerJoinInSparkRDD() {
        final var innerJoinRdd = customersPairs.join(billsPairs);

        innerJoinRdd.collect().forEach(System.out::println);
        System.out.println("--------------------");
        innerJoinRdd.collect().forEach(j -> System.out.printf("%s had order of %.2f dollars%n", j._2._1, j._2._2));
        System.out.println("--------------------");
    }

    @Test
    @DisplayName("Test Left Outer Join in Spark RDD")
    void testLeftOuterJoinInSparkRDD() {
        final var leftOuterJoinRdd = customersPairs.leftOuterJoin(billsPairs);

        leftOuterJoinRdd.collect().forEach(System.out::println);
        System.out.println("--------------------");
        leftOuterJoinRdd.collect().forEach(j -> {
            if (j._2._2.isPresent()) {
                System.out.printf("%s had order of %.2f dollars%n", j._2._1, j._2._2.get());
            } else {
                System.out.printf("%s had no order%n", j._2._1);
            }
        });
        System.out.println("--------------------");
    }

    @Test
    @DisplayName("Test Right Outer Join in Spark RDD")
    void testRightOuterJoinInSparkRDD() {
        final var rightOuterJoinRdd = customersPairs.rightOuterJoin(billsPairs);

        rightOuterJoinRdd.collect().forEach(System.out::println);
        System.out.println("--------------------");
        rightOuterJoinRdd.collect().forEach(j -> {
            if (j._2._1.isPresent()) {
                System.out.printf("%s had order of %.2f dollars%n", j._2._1.get(), j._2._2);
            } else {
                System.out.printf("Do NOT know who placed the order of %.2f%n", j._2._2);
            }
        });
        System.out.println("--------------------");
    }

    @Test
    @DisplayName("Test Full Outer Join in Spark RDD")
    void testFullOuterJoinInSparkRDD() {
        final var fullOuterJoinRdd = customersPairs.fullOuterJoin(billsPairs);

        fullOuterJoinRdd.collect().forEach(System.out::println);
        System.out.println("--------------------");
        fullOuterJoinRdd.collect().forEach(j -> {
            if (j._2._1.isPresent()) {
                if (j._2._2.isPresent()) {
                    System.out.printf("%s had order of %.2f dollars%n", j._2._1.get(), j._2._2.get());
                } else {
                    System.out.printf("%s had no order%n", j._2._1.get());
                }
            } else {
                if (j._2._2.isPresent()) {
                    System.out.printf("Do NOT know who placed the order of %.2f%n", j._2._2.get());
                } else {
                    System.out.printf("Unknown record%n"); // this will never be printed
                }
            }
        });
        System.out.println("--------------------");
    }

    @Test
    @DisplayName("Test Cartesian Join in Spark RDD")
    void testCartesianJoinInSparkRDD() {
        final var cartesianJoinRdd = customersPairs.cartesian(billsPairs);

        cartesianJoinRdd.collect().forEach(System.out::println);
        System.out.println("--------------------");
    }

}
