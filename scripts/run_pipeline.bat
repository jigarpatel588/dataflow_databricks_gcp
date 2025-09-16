@echo off
REM GCS to Databricks Pipeline Runner Script for Windows
REM Make sure to update the configuration parameters before running

REM Configuration - Update these values
set PROJECT_ID=your-gcp-project-id
set INPUT_FILE=gs://your-gcp-project-id/data/products.csv
set DATABRICKS_HOST=your-databricks-host.cloud.databricks.com
set DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
set DATABRICKS_TOKEN=your-databricks-access-token
set DATABRICKS_DATABASE=default
set DATABRICKS_TABLE=products
set TEMP_LOCATION=gs://your-gcp-project-id/temp
set STAGING_LOCATION=gs://your-gcp-project-id/staging

REM Build the project
echo Building the project...
mvn clean compile package

REM Run the pipeline
echo Running GCS to Databricks pipeline...
mvn exec:java -Dexec.mainClass="com.example.GcsToDatabricksPipeline" ^
  -Dexec.args="--project=%PROJECT_ID% ^
  --inputFile=%INPUT_FILE% ^
  --databricksHost=%DATABRICKS_HOST% ^
  --databricksHttpPath=%DATABRICKS_HTTP_PATH% ^
  --databricksToken=%DATABRICKS_TOKEN% ^
  --databricksDatabase=%DATABRICKS_DATABASE% ^
  --databricksTable=%DATABRICKS_TABLE% ^
  --tempLocation=%TEMP_LOCATION% ^
  --stagingLocation=%STAGING_LOCATION% ^
  --runner=SparkRunner"

echo Pipeline execution completed!
pause
