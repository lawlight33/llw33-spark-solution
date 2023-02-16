package com.epam;

import org.apache.spark.sql.SparkSession;

public class AppRunner {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("SparkSolution").getOrCreate();
        new Solution().solve(sparkSession, args[0], args[1]);
    }
}
