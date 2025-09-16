#!/bin/bash

# GCS to Databricks Pipeline Runner Script
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
STAGING_LOCATION="gs://your-gcp-project-id/staging"

# Build the project
echo "Building the project..."
mvn clean compile package

# Run the pipeline
echo "Running GCS to Databricks pipeline..."
mvn exec:java -Dexec.mainClass="com.example.GcsToDatabricksPipeline" \
  -Dexec.args="--project=$PROJECT_ID \
  --inputFile=$INPUT_FILE \
  --databricksHost=$DATABRICKS_HOST \
  --databricksHttpPath=$DATABRICKS_HTTP_PATH \
  --databricksToken=$DATABRICKS_TOKEN \
  --databricksDatabase=$DATABRICKS_DATABASE \
  --databricksTable=$DATABRICKS_TABLE \
  --tempLocation=$TEMP_LOCATION \
  --stagingLocation=$STAGING_LOCATION \
  --runner=SparkRunner"

echo "Pipeline execution completed!"
