#!/bin/bash

# GCS to Databricks Spark Pipeline Runner Script
# Make sure to update the configuration parameters before running

# Configuration - Update these values
PROJECT_ID="your-gcp-project-id"
INPUT_FILE="gs://your-gcp-project-id/data/products.csv"
DATABRICKS_HOST="your-databricks-host.cloud.databricks.com"
DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
DATABRICKS_TOKEN="your-databricks-access-token"
DATABRICKS_DATABASE="default"
DATABRICKS_TABLE="products"
TEMP_LOCATION="gs://your-gcp-project-id/temp"
BIGQUERY_DATASET="temp_dataset"
BIGQUERY_TABLE="temp_products"

# Spark Configuration
SPARK_MASTER="local[*]"  # Use local[*] for local testing, or set to cluster URL for production
SPARK_DRIVER_MEMORY="2g"
SPARK_EXECUTOR_MEMORY="2g"
SPARK_EXECUTOR_CORES="2"

# Build the project
echo "Building the project..."
mvn clean compile package

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed. Exiting."
    exit 1
fi

# Run the Spark pipeline
echo "Running GCS to Databricks Spark pipeline..."
mvn exec:java -Dexec.mainClass="com.example.GcsToDatabricksSparkPipeline" \
  -Dexec.args="$PROJECT_ID \
  $INPUT_FILE \
  $BIGQUERY_DATASET \
  $BIGQUERY_TABLE \
  $DATABRICKS_HOST \
  $DATABRICKS_HTTP_PATH \
  $DATABRICKS_TOKEN \
  $DATABRICKS_DATABASE \
  $DATABRICKS_TABLE \
  $TEMP_LOCATION" \
  -Dspark.master="$SPARK_MASTER" \
  -Dspark.driver.memory="$SPARK_DRIVER_MEMORY" \
  -Dspark.executor.memory="$SPARK_EXECUTOR_MEMORY" \
  -Dspark.executor.cores="$SPARK_EXECUTOR_CORES"

echo "Spark pipeline execution completed!"
