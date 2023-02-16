package com.epam;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * IMPORTANT: MUST SPECIFY PARTITION_COUNT VARIABLE
 * -----------------------------------------
 * [ CURRENT ALGORITHM : sorted-partitions ]
 * -----------------------------------------
 *
 * There are 3 possible algorithms:
 *
 *  1. GIT BRANCH NAME: group-by
 *    - make PairRDD by company
 *    - groupByKey(company)
 *    - sortBy(number).over(company)
 *    - iterate over each company row, enrich with prev number & value
 *    - sortBy(number).overAll
 *
 *  2. GIT BRANCH NAME: aggregate
 *    - make PairRDD by company
 *    - aggregateByKey.
 *         * collect lineage for each company within each partition. CompanyLineage::addRecord
 *         * merge partition company lineages. CompanyLineage::merge
 *         * keep lineage sorted
 *    - enrich each collected lineage for each company with prev number & prev value. CompanyLineage::compile
 *    - sortBy(number).overAll
 *
 *  3. GIT BRANCH NAME: sorted-partitions <---- current
 *    - make PairRDD by company_number
 *    - repartitionAndSortWithinPartitions
 *         * put company data inside one partition (multiple companies inside partition is also ok)
 *         * sort each partition by number (not considering company)
 *    - mapPartition
 *         * iterate over elements inside each partition
 *         * prepare new enriched partition with prev number & prev value
 *    - sortByKey
 *
 * Each solution was tested on AWS EMR 6.7.0 cluster with S3 as datasource.
 * AWS EMR Spark cluster spec: 1x driver, 2x executors. VM spec: 4x core, 16 GB x RAM.
 * Used 20-million post-processed dataset from Kaggle: daily-historical-stock-prices-1970-2018.
 * Prepared 2 files, each file contains 10 million rows.
 *
 * ------------------------------------------------
 * | group-by | aggregate | sorted-partitions     |
 * ------------------------------------------------
 * |  3 min   |   5 min   | 3 min (16 partitions) |
 * ------------------------------------------------
 *
 * All results passed cross-equals check: result1DF.exceptAll(result2DF).count() == 0   <---- true for each result pair
 * Also there are 4 unit tests:
 *    1. SolutionTest::demoTestInput() for given input (1,APPLE,2000, 2,GOOGLE,10, 3,MICROSOFT,5000 ...)
 *    2. SolutionTest::enrichPartitionTest() for enrich partition map function
 *    3. CompanyNamePartitionerTest::partitionTest() for company Partitioner class
 *    4. CompanyNumberComparatorTest::compareTest() for company Comparator class
 *
 * How to run:
 * 1. Choose wanted algorithm by checkout appropriate git branch.
 * 2. If running with AWS EMR cluster: uncomment AWS EMR sections in pom.xml
 * 3. execute: spark-submit ... com.epam.AppRunner spark-solution.jar "inputDir" "outputDir"
 */

public class Solution {

    // TODO must specify partitions count considering actual data distribution (data skew) and Spark cluster parameters
    // 16 partitions is the best value for:
    //   - 2x files, 10 million rows each file
    //   - 2x executors, 1x driver, each VM has 4x CPU, 10 GB x RAM
    //   - "real" stock prices companies data skew: Kaggle's daily-historical-stock-prices-1970-2018 dataset
    private static final int PARTITION_COUNT = 16;

    public void solve(SparkSession sparkSession, String inputDirectory, String outputDirectory) {
        JavaPairRDD<String, Row> javaPairRDD = sparkSession.read()
                .csv(inputDirectory + "/*").javaRDD()
                .keyBy(r -> r.getAs(COMPANY_POS) + "_" + r.get(NUMBER_POS));

        JavaPairRDD<String, Row> rr = javaPairRDD.repartitionAndSortWithinPartitions(
                new CompanyNamePartitioner(PARTITION_COUNT),
                new CompanyNumberComparator());

        rr.mapPartitionsToPair(flatMap, true)
                .sortByKey()
                .map(new CsvTransformer())
                .saveAsTextFile(outputDirectory);

        // renameFiles(outputDirectory); TODO uncomment for pretty output filenames and .csv extensions
    }

    final PairFlatMapFunction<Iterator<Tuple2<String, Row>>, Long, Row> flatMap = rows -> {
        List<Tuple2<Long, Row>> newRows = new ArrayList<>();
        Map<String, Tuple2<Long, Long>> cache = new HashMap<>();
        while (rows.hasNext()) {
            Tuple2<String, Row> tuple = rows.next();
            String company = tuple._2.getAs(COMPANY_POS);
            long number = Long.parseLong(tuple._2.getAs(NUMBER_POS));
            long value = Long.parseLong(tuple._2.getAs(VALUE_POS));
            Tuple2<Long, Long> previous = cache.getOrDefault(company, new Tuple2<>(0L, 0L));
            Row newRow = RowFactory.create(number, company, value, previous._1, previous._2);
            if (value > 1000) {
                cache.put(company, new Tuple2<>(number, value));
            }
            newRows.add(new Tuple2<>(number, newRow));
        }
        return newRows.iterator();
    };

    private static class CsvTransformer implements Function<Tuple2<Long, Row>, String> {
        @Override
        public String call(Tuple2<Long, Row> v1) {
            String row = v1._2.toString();
            return row.substring(1, row.length() - 1);
        }
    }

    private void renameFiles(String outputDir) {
        for (File file : Objects.requireNonNull(new File(outputDir).listFiles())) {
            try {
                String oldName = file.getName();
                if (!oldName.startsWith("part-")) {
                    Preconditions.checkArgument(file.delete(), "Unable to remove file");
                    continue;
                }
                int index = Integer.parseInt(oldName.split("-")[1]);
                String newName = String.format("output-%s.csv", index);
                File newFileName = new File(file.getParent() + "/" + newName);
                Preconditions.checkArgument(file.renameTo(newFileName), "Rename was failed");
            } catch (Exception ex) {
                throw new RuntimeException("Unable to rename " + file.getName(), ex);
            }
        }
    }

    private static final int NUMBER_POS = 0;
    private static final int COMPANY_POS = 1;
    private static final int VALUE_POS = 2;
}