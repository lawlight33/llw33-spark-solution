package com.epam;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Prerequisites:
 *   - Set IntelliJ IDEA project JDK 1.8 before test run.
 *   - Local Spark binaries, %SPARK_HOME%, PATH
 *   - Local Hadoop binaries for Windows: winutils.exe, %HADOOP_HOME%, PATH
 */
public class SolutionTest {

    @Test
    void demoTestInput() throws IOException {
        String input = "src/test/resources/input";
        String output = "src/test/resources/output";

        String expectedOutput =
                "1,APPLE,2000,0,0\n" +
                        "2,GOOGLE,10,0,0\n" +
                        "3,MICROSOFT,5000,0,0\n" +
                        "4,APPLE,100,1,2000\n" +
                        "5,GOOGLE,2000,0,0\n" +
                        "6,MICROSOFT,3000,3,5000\n" +
                        "7,GOOGLE,100,5,2000\n" +
                        "8,GOOGLE,200,5,2000\n";

        FileUtils.deleteDirectory(new File(output));

        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .config("spark.driver.host","127.0.0.1")
                .config("spark.driver.bindAddress","127.0.0.1")
                .config("spark.default.parallelism", 2)
                .appName("SolutionTest")
                .getOrCreate();
        new Solution().solve(sparkSession, input, output);

        StringBuilder actualOutput = new StringBuilder();
        for (File file : Objects.requireNonNull(new File(output).listFiles())) {
            if (file.getName().contains(".") || !file.getName().startsWith("part-")) {
                continue;
            }
            Files.readAllLines(file.getAbsoluteFile().toPath()).forEach(line -> actualOutput.append(line).append("\n"));
        }

        System.out.println(actualOutput);

        assertEquals(expectedOutput, actualOutput.toString());
    }

    @Test
    public void enrichPartitionTest() throws Exception {
        PairFlatMapFunction<Iterator<Tuple2<String, Row>>, Long, Row> enrichPartition = new Solution().flatMap;

        List<Tuple2<String, Row>> partitionData = new ArrayList<>();
        partitionData.add(new Tuple2<>("APPLE_1", RowFactory.create("1", "APPLE", "2000")));
        partitionData.add(new Tuple2<>("GOOGLE_2", RowFactory.create("2", "GOOGLE", "10")));
        partitionData.add(new Tuple2<>("APPLE_4", RowFactory.create("4", "APPLE", "100")));
        partitionData.add(new Tuple2<>("GOOGLE_5", RowFactory.create("5", "GOOGLE", "2000")));
        partitionData.add(new Tuple2<>("GOOGLE_7", RowFactory.create("7", "GOOGLE", "100")));
        partitionData.add(new Tuple2<>("GOOGLE_8", RowFactory.create("8", "GOOGLE", "200")));

        List<Tuple2<Long, Row>> actualOutput = Lists.newArrayList(enrichPartition.call(partitionData.iterator()));

        List<Tuple2<Long, Row>> expected = new ArrayList<>();
        expected.add(new Tuple2<>(1L, RowFactory.create("1", "APPLE", "2000", "0", "0")));
        expected.add(new Tuple2<>(2L, RowFactory.create("2", "GOOGLE", "10", "0", "0")));
        expected.add(new Tuple2<>(4L, RowFactory.create("4", "APPLE", "100", "1", "2000")));
        expected.add(new Tuple2<>(5L, RowFactory.create("5", "GOOGLE", "2000", "0", "0")));
        expected.add(new Tuple2<>(7L, RowFactory.create("7", "GOOGLE", "100", "5", "2000")));
        expected.add(new Tuple2<>(8L, RowFactory.create("8", "GOOGLE", "200", "5", "2000")));

        System.out.println(actualOutput);
        assertEquals(expected.toString(), actualOutput.toString());
    }
}
