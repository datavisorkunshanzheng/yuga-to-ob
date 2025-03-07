#!/bin/bash

# Enable debug mode
set -x

# Exit on error
set -e

echo "Starting export process..."

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed. Please install Maven first."
    exit 1
fi

# Check if Spark is installed
if ! command -v spark-submit &> /dev/null; then
    echo "Error: spark-submit not found. Please ensure Apache Spark is installed and in your PATH."
    exit 1
fi

echo "Building project with Maven..."
# Build the project first
mvn clean package || {
    echo "Error: Maven build failed"
    exit 1
}

# Default values
CASSANDRA_HOST="localhost"
CASSANDRA_PORT="9042"
OUTPUT_DIR="$(pwd)/export_data"
SPARK_PACKAGES="com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
SQL_EXTENSIONS="com.datastax.spark.connector.CassandraSparkExtensions"

echo "Using configuration:"
echo "Cassandra Host: $CASSANDRA_HOST"
echo "Cassandra Port: $CASSANDRA_PORT"
echo "Output Directory: $OUTPUT_DIR"
echo "Spark Packages: $SPARK_PACKAGES"

# Check if the JAR file exists
JAR_FILE="target/export_db-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found at $JAR_FILE"
    exit 1
fi

# Create output directory if it doesn't exist
echo "Creating output directory..."
mkdir -p "$OUTPUT_DIR"

# Test Cassandra connection
echo "Testing Cassandra connection..."
nc -z -w5 "$CASSANDRA_HOST" "$CASSANDRA_PORT" || {
    echo "Error: Cannot connect to Cassandra at $CASSANDRA_HOST:$CASSANDRA_PORT"
    echo "Please ensure YugabyteDB is running and accessible"
    exit 1
}

echo "Submitting Spark job..."
# Run the Spark job
spark-submit \
  --class com.export.dataExport \
  --master local[*] \
  --packages "$SPARK_PACKAGES" \
  --conf "spark.sql.extensions=$SQL_EXTENSIONS" \
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=4g" \
  "$JAR_FILE" \
  --spark_master local[*] \
  --cassandra_host "$CASSANDRA_HOST" \
  --cassandra_port "$CASSANDRA_PORT" \
  --output_path "$OUTPUT_DIR" \
  "$@" || {
    echo "Error: Spark job failed"
    exit 1
}

# Check if export was successful
if [ -f "$OUTPUT_DIR/failed_tables.txt" ]; then
    echo "Warning: Some tables failed to export. Check $OUTPUT_DIR/failed_tables.txt for details"
    cat "$OUTPUT_DIR/failed_tables.txt"
else
    echo "Export completed successfully. Data exported to: $OUTPUT_DIR"
fi

# Disable debug mode
set +x 