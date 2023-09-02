package com.backstreetbrogrammer.chapter15_cachePersist;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class RDDCachePersistenceBenchmarking {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDCachePersistenceBenchmarking")
                                                       .setMaster("local[*]");
    private final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz")
                                            .toString();

    @Benchmark
    public long groupByKeyWithoutCacheOrPersist() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var counts = pairRDD.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return pairRDD.count();
        }
    }

    @Benchmark
    public long groupByKeyWithCache() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var cachedPairRdd = pairRDD.cache();
            final var counts = cachedPairRdd.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return cachedPairRdd.count();
        }
    }

    @Benchmark
    public long groupByKeyWithPersistMemoryOnly() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var persistedPairRdd = pairRDD.persist(StorageLevel.MEMORY_ONLY());
            final var counts = persistedPairRdd.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return persistedPairRdd.count();
        }
    }

    @Benchmark
    public long groupByKeyWithPersistMemoryAndDisk() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var persistedPairRdd = pairRDD.persist(StorageLevel.MEMORY_AND_DISK());
            final var counts = persistedPairRdd.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return persistedPairRdd.count();
        }
    }

    @Benchmark
    public long groupByKeyWithPersistMemoryOnlySer() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var persistedPairRdd = pairRDD.persist(StorageLevel.MEMORY_ONLY_SER());
            final var counts = persistedPairRdd.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return persistedPairRdd.count();
        }
    }

    @Benchmark
    public long groupByKeyWithPersistMemoryAndDiskSer() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var persistedPairRdd = pairRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
            final var counts = persistedPairRdd.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return persistedPairRdd.count();
        }
    }

    @Benchmark
    public long groupByKeyWithPersistDiskOnly() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var persistedPairRdd = pairRDD.persist(StorageLevel.DISK_ONLY());
            final var counts = persistedPairRdd.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return persistedPairRdd.count();
        }
    }

    @Benchmark
    public long groupByKeyWithPersistOffHeap() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var persistedPairRdd = pairRDD.persist(StorageLevel.OFF_HEAP());
            final var counts = persistedPairRdd.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return persistedPairRdd.count();
        }
    }


    @Benchmark
    public long groupByKeyWithPersistNone() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var lines = sparkContext.textFile(testFilePath);
            final var pairRDD = lines.mapToPair(line -> new Tuple2<>(line.length(), 1L));
            final var persistedPairRdd = pairRDD.persist(StorageLevel.NONE());
            final var counts = persistedPairRdd.groupByKey();
            counts.take(1).forEach(tuple -> {
                final Integer integer = tuple._1;
                final int size = Iterables.size(tuple._2);
            });
            return persistedPairRdd.count();
        }
    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(RDDCachePersistenceBenchmarking.class.getName())
                .build();
        new Runner(opt).run();
    }
}
