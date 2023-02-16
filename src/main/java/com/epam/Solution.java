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
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

/**
 * ---------------------------------
 * [ CURRENT ALGORITHM : group-by  ]
 * ---------------------------------
 *
 * There are 3 possible algorithms:
 *
 *  1. GIT BRANCH NAME: group-by <---- current
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

        JavaPairRDD<Long, Row> processedRDD = javaPairRDD
                .groupByKey()
                .flatMapToPair(enrich)
                .sortByKey();

        processedRDD.map(new CsvTransformer())
                .saveAsTextFile(outputDirectory);

        // renameFiles(outputDirectory); TODO uncomment for pretty output filenames and .csv extensions
    }

    private final PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, Row> enrich = companyRecords -> {
        String company = companyRecords._1();
        AtomicLong prevValue = new AtomicLong(0L);
        AtomicLong prevNumber = new AtomicLong(0L);
        List<Tuple2<Long, Row>> tuples = new ArrayList<>();
        StreamSupport.stream(companyRecords._2.spliterator(), false)
                .sorted(Comparator.comparingLong(o -> Long.parseLong(o.getAs(NUMBER_POS))))
                .forEach(record -> {
                    Long number = Long.parseLong(record.getAs(NUMBER_POS));
                    Long value = Long.parseLong(record.getAs(VALUE_POS));
                    Row row = RowFactory.create(company, value, prevNumber.get(), prevValue.get());
                    tuples.add(new Tuple2<>(number, row));
                    if (value > 1000) {
                        prevNumber.set(number);
                        prevValue.set(value);
                    }
                });
        return tuples.iterator();
    };

    private static class CsvTransformer implements Serializable, Function<Tuple2<Long, Row>, String> {
        @Override
        public String call(Tuple2<Long, Row> v1) {
            String row = v1._2.toString();
            row = row.substring(1, row.length() - 1);
            return v1._1 + "," + row;
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
