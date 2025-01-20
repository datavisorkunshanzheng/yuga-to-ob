package com.export;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.hex;
import static org.apache.spark.sql.functions.to_json;

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

public class dataExport {
    private static final Logger logger = LoggerFactory.getLogger(dataExport.class);
    private static Map<String, String> params = new HashMap<>();
    private static List<String> failedTables = new ArrayList<>();

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
        if (!params.containsKey("cassandra_host")) {
            throw new Exception("Missing cassandra_host parameter");
        }
        if (!params.containsKey("cassandra_port")) {
            throw new Exception("Missing cassandra_port parameter");
        }
        if (!params.containsKey("output_path")) {
            logger.warn("Missing output_path parameter, will use current directory");
            params.put("output_path", System.getProperty("user.dir"));
        }
    }

    static SparkSession createSparkSession() throws Exception {
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("DB reader")
                .master(params.get("spark_master"))
                .config("spark.cassandra.connection.host", params.get("cassandra_host"))
                .config("spark.cassandra.connection.port", params.get("cassandra_port"));

        if (params.containsKey("jar_packages")) {
            sparkBuilder.config("spark.jars.packages", params.get("jar_packages"));
        }

        if (params.containsKey("sql_extensions")) {
            sparkBuilder.config("spark.sql.extensions", params.get("sql_extensions"));
        }

        return sparkBuilder.getOrCreate();
    }

    static Map<String, List<String>> retrieveTableNames() throws Exception {
        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(params.get("cassandra_host"),
                        Integer.parseInt(params.get("cassandra_port"))))
                .withLocalDatacenter("datacenter1")
                .build();

        ResultSet rs = session.execute(
                "SELECT keyspace_name, table_name FROM system_schema.tables");
        Map<String, List<String>> tableNames = new HashMap<>();
        rs.forEach(row -> {
            if (row.getString("keyspace_name").equals("system_schema") || row.getString(
                    "keyspace_name").equals("system") || row.getString("keyspace_name")
                    .equals("system_auth")) {
                return;
            }
            String keyspace = row.getString("keyspace_name");
            String table = row.getString("table_name");

            if (table == null || table.startsWith("user_events") || table.startsWith(
                    "user_events_index") || table.startsWith("events")) {
                return;
            }

            if (!tableNames.containsKey(keyspace)) {
                tableNames.put(keyspace, new ArrayList<>());
            }
            tableNames.get(keyspace).add(table);
        });

        session.close();

        return tableNames;
    }

    static void exportOneTableCassandra(SparkSession spark, String keyspace, String table) {
        Dataset<Row> data = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("table", table)
                .option("keyspace", keyspace)
                .load();

        try {
            data = transformMapTypeColumn(data);
        } catch (Exception e) {
            logger.error("Error transforming map type column", e);
        }

        if (table.startsWith("velocity_")){
            data = data.select("dim", "accu_id", "time", "detail");
        }

        data.write().option("header", "true")
                .mode("overwrite")
                .orc(params.get("output_path") + "/" + keyspace + "/" + table);
    }

    static Dataset<Row> transformMapTypeColumn(Dataset<Row> data) {
        StructField[] fields = data.schema().fields();
        for (StructField field : fields) {
            if (field.dataType() instanceof MapType) {
                data = data.withColumn(field.name(), to_json(data.col(field.name())));
            } else if (field.dataType() instanceof BinaryType) {
                data = data.withColumn(field.name(), hex(data.col(field.name())));
            }
        }
        return data;
    }

    public static void main(String[] args) throws Exception {
        argToProperties(args);
        validateProperties();
        SparkSession spark = createSparkSession();

        Map<String, List<String>> tableNames = new HashMap<>();
        if (params.containsKey("table_list")) {
            String[] tables = params.get("table_list").split(",");
            for (String table : tables) {
                String[] parts = table.split("\\.");
                if (parts.length != 2) {
                    throw new Exception("Invalid table name: " + table);
                }
                String keyspace = parts[0];
                String tableName = parts[1];
                if (!tableNames.containsKey(keyspace)) {
                    tableNames.put(keyspace, new ArrayList<>());
                }
                tableNames.get(keyspace).add(tableName);
            }
        } else {
            tableNames = retrieveTableNames();
        }

        for (String keyspace : tableNames.keySet()) {
            for (String table : tableNames.get(keyspace)) {
                try {
                    exportOneTableCassandra(spark, keyspace, table);
                } catch (Exception e) {
                    // retry once
                    try {
                        exportOneTableCassandra(spark, keyspace, table);
                    } catch (Exception e2) {
                        String failedTable = keyspace + "." + table;
                        logger.error("################# Export table failed after one retry"
                                + failedTable, e2);
                        failedTables.add(failedTable);
                    }
                }
                logger.info("################## Export table success " + keyspace + "." + table);
            }
        }

        spark.stop();

        // Start record failed tables
        String fileName = params.get("output_path") + "/failed_tables.txt";
        if (!failedTables.isEmpty()) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
                writer.write(String.join(",", failedTables));
            } catch (Exception e) {
                System.err.println("Error writing to file: " + e.getMessage());
            }
        }
    }
}