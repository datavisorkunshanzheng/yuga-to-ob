package com.export;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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

public class checkDup {
    private static final Logger logger = LoggerFactory.getLogger(checkDup.class);
    private static Map<String, String> params = new HashMap<>();

    static void argToProperties(String[] args) {
        String name = "";
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                name = arg.substring(2);
            } else if (!name.isEmpty()) {
                params.put(name, arg);
            }
        }
    }

    static void validateProperties() throws Exception {
        if (!params.containsKey("spark_master")) {
            throw new Exception("Missing spark_master parameter");
        }
        if (!params.containsKey("output_path")) {
            logger.warn("Missing output_path parameter, will use current directory");
            params.put("output_path", System.getProperty("user.dir"));
        }
        if (!params.containsKey("input_s3")) {
            throw new Exception("Missing input_s3 parameter");
        }
        if (!params.containsKey("input_s3_folder")) {
            throw new Exception("Missing input_s3_folder parameter");
        }

        if (!params.containsKey("aws_access_key")) {
            throw new Exception("Missing aws_access_key parameter");
        }
        if (!params.containsKey("aws_secret_key")) {
            throw new Exception("Missing aws_secret_key parameter");
        }
        if (!params.containsKey("aws_region")) {
            throw new Exception("Missing aws_region parameter");
        }
    }

    public static void main(String[] args) throws Exception {
        argToProperties(args);
        validateProperties();

        SparkConf conf = new SparkConf().setAppName("DataExport")
                .setMaster(params.get("spark_master"));
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> df = spark.read().option("header", "true")
                .orc("s3a://" + params.get("input_s3") + "/" + params.get("input_s3_folder")
                        + "/*.orc");
        Dataset<Row> groupedDf = df.groupBy("dim", "accu_id", "time")
                .agg(functions.count("*").as("count"));

        Dataset<Row> dupDf = groupedDf.filter("count > 1");

        dupDf.write().option("header", "true")
                .mode("overwrite")
                .csv(params.get("output_path"));

        spark.stop();
    }
}