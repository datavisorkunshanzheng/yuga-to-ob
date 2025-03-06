package com.export;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*************************************************************************
 *
 * Copyright (c) 2016, DATAVISOR, INC.
 * All rights reserved.
 * __________________
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of DataVisor, Inc.
 * The intellectual and technical concepts contained
 * herein are proprietary to DataVisor, Inc. and
 * may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from DataVisor, Inc.
 */

public class checkDupTest {
    private static final Logger logger = LoggerFactory.getLogger(checkDupTest.class);


    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("DataExport").setMaster("local");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        Dataset<Row> df = spark.read().option("header", "true").csv("export_yuga/src/main/resources/test.csv");
        Dataset<Row> groupedDf = df.groupBy("dim", "accu_id", "time").agg(functions.count("*").as("count"));

        Dataset<Row> dupDf = groupedDf.filter("count > 1");

        dupDf.show();

        spark.stop();
    }
}