package com.epam;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
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
    public void companyLineageTest() {
        Solution.CompanyLineage completeGoogleLineage;

        Solution.CompanyLineage googleLineagePartition1 = new Solution.CompanyLineage();
        Solution.CompanyLineage googleLineagePartition2 = new Solution.CompanyLineage();

        // partition1
        List<Row> companyRecordsPartition1 = new ArrayList<>();
        companyRecordsPartition1.add(RowFactory.create("5", "GOOGLE", "2000"));
        companyRecordsPartition1.add(RowFactory.create("2", "GOOGLE", "10"));
        for (Row record : companyRecordsPartition1) {
            googleLineagePartition1.addRecord(record);
        }

        // partition2
        List<Row> companyRecordsPartition2 = new ArrayList<>();
        companyRecordsPartition2.add(RowFactory.create("8", "GOOGLE", "200"));
        companyRecordsPartition2.add(RowFactory.create("7", "GOOGLE", "100"));
        for (Row record : companyRecordsPartition2) {
            googleLineagePartition2.addRecord(record);
        }

        // merge partitions & compile lineage (enrich with historical data)
        completeGoogleLineage = googleLineagePartition2.merge(googleLineagePartition1);
        List<Tuple2<Long, RowWrapper>> actualOutput = Lists.newArrayList(completeGoogleLineage.compile());

        List<Tuple2<Long, RowWrapper>> expected = new ArrayList<>();
        expected.add(new Tuple2<>(2L, new RowWrapper(RowFactory.create("2", "GOOGLE", "10", "0", "0"))));
        expected.add(new Tuple2<>(5L, new RowWrapper(RowFactory.create("5", "GOOGLE", "2000", "0", "0"))));
        expected.add(new Tuple2<>(7L, new RowWrapper(RowFactory.create("7", "GOOGLE", "100", "5", "2000"))));
        expected.add(new Tuple2<>(8L, new RowWrapper(RowFactory.create("8", "GOOGLE", "200", "5", "2000"))));

        System.out.println(actualOutput);
        assertEquals(expected.toString(), actualOutput.toString());
    }
}
