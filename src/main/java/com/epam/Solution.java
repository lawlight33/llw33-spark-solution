package com.epam;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * ---------------------------------
 * [ CURRENT ALGORITHM : aggregate ]
 * ---------------------------------
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
 *  2. GIT BRANCH NAME: aggregate <---- current
 *    - make PairRDD by company
 *    - aggregateByKey.
 *         * collect lineage for each company within each partition. CompanyLineage::addRecord
 *         * merge partition company lineages. CompanyLineage::merge
 *         * keep lineage sorted
 *    - enrich each collected lineage for each company with prev number & prev value. CompanyLineage::compile
 *    - sortBy(number).overAll
 *
 *  3. GIT BRANCH NAME: sorted-partitions
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
 * AWS EMR Spark cluster spec: 1x driver, 2x executors. VM spec: 4x core, 16xGB RAM.
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
 * Also there are 2 unit tests:
 *    1. SolutionTest::demoTestInput() for given input (1,APPLE,2000, 2,GOOGLE,10, 3,MICROSOFT,5000 ...)
 *    2. SolutionTest::companyLineageTest() for company linage class (CompanyLineage)
 *
 * How to run:
 * 1. Choose wanted algorithm by checkout appropriate git branch.
 * 2. If running with AWS EMR cluster: uncomment AWS EMR sections in pom.xml
 * 3. execute: spark-submit ... com.epam.AppRunner spark-solution.jar "inputDir" "outputDir"
 */

public class Solution {

    public void solve(SparkSession sparkSession, String inputDirectory, String outputDirectory) {
        JavaPairRDD<String, Row> javaPairRDD = sparkSession.read()
                .csv(inputDirectory + "/*").javaRDD()
                .keyBy(row -> row.getAs(COMPANY_POS));

        JavaPairRDD<String, CompanyLineage> accumulators = javaPairRDD.aggregateByKey(
                new CompanyLineage(),
                CompanyLineage::addRecord,
                CompanyLineage::merge);

        accumulators.flatMapToPair(entry -> entry._2.compile())
                .sortByKey()
                .map(new CsvTransformer())
                .saveAsTextFile(outputDirectory);

        // renameFiles(outputDirectory); TODO uncomment for pretty output filenames and .csv extensions
    }

    static class CompanyLineage implements Serializable {
        TreeMap<Long, RowWrapper> cache = new TreeMap<>();

        public CompanyLineage addRecord(Row row) {
            long number = Long.parseLong(row.getAs(NUMBER_POS));
            this.cache.put(number, new RowWrapper(row));
            return this;
        }

        public CompanyLineage merge(CompanyLineage companyLineage) {
            this.cache.putAll(companyLineage.cache);
            return this;
        }

        public Iterator<Tuple2<Long, RowWrapper>> compile() {
            enrichWithPrevious();
            return makePairs();
        }

        private void enrichWithPrevious() {
            long prevNumber = 0L;
            long prevValue = 0L;
            for (Map.Entry<Long, RowWrapper> e : cache.entrySet()) {
                String curNumberStr = e.getValue().getAs(NUMBER_POS);
                String company = e.getValue().getAs(COMPANY_POS);
                String curValueStr = e.getValue().getAs(VALUE_POS);
                long curNumber = Long.parseLong(curNumberStr);
                long curValue = Long.parseLong(curValueStr);
                String prevNumberStr = String.valueOf(prevNumber);
                String prevValueStr = String.valueOf(prevValue);
                Row newRow = RowFactory.create(curNumberStr, company, curValueStr, prevNumberStr, prevValueStr);
                e.getValue().modifyRow(newRow);
                if (curValue > 1000) {
                    prevValue = curValue;
                    prevNumber = curNumber;
                }
            }
        }

        private Iterator<Tuple2<Long, RowWrapper>> makePairs() {
            return cache.entrySet().stream()
                    .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                    .iterator();
        }

        @Override
        public String toString() {
            return cache.toString();
        }
    }

    private static class CsvTransformer implements Serializable, Function<Tuple2<Long, RowWrapper>, String> {
        @Override
        public String call(Tuple2<Long, RowWrapper> v1) {
            return v1._2.toString();
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
