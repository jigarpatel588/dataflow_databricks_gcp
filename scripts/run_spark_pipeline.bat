@echo off
REM GCS to Databricks Spark Pipeline Runner Script for Windows
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
set BIGQUERY_DATASET=temp_dataset
set BIGQUERY_TABLE=temp_products

REM Spark Configuration
set SPARK_MASTER=local[*]
set SPARK_DRIVER_MEMORY=2g
set SPARK_EXECUTOR_MEMORY=2g
set SPARK_EXECUTOR_CORES=2

REM Build the project
echo Building the project...
mvn clean compile package

REM Check if build was successful
if %ERRORLEVEL% neq 0 (
    echo Build failed. Exiting.
    exit /b 1
)

REM Run the Spark pipeline
echo Running GCS to Databricks Spark pipeline...
mvn exec:java -Dexec.mainClass="com.example.GcsToDatabricksSparkPipeline" ^
  -Dexec.args="%PROJECT_ID% ^
  %INPUT_FILE% ^
  %BIGQUERY_DATASET% ^
  %BIGQUERY_TABLE% ^
  %DATABRICKS_HOST% ^
  %DATABRICKS_HTTP_PATH% ^
  %DATABRICKS_TOKEN% ^
  %DATABRICKS_DATABASE% ^
  %DATABRICKS_TABLE% ^
  %TEMP_LOCATION%" ^
  -Dspark.master="%SPARK_MASTER%" ^
  -Dspark.driver.memory="%SPARK_DRIVER_MEMORY%" ^
  -Dspark.executor.memory="%SPARK_EXECUTOR_MEMORY%" ^
  -Dspark.executor.cores="%SPARK_EXECUTOR_CORES%"

echo Spark pipeline execution completed!
pause
