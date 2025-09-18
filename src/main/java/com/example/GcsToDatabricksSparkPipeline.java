/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.spark.SparkConf;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.storage.*;


/**
 * Spark-based pipeline that reads CSV data from Google Cloud Storage
 * and loads it into Databricks using native Spark APIs.
 */
public class GcsToDatabricksSparkPipeline {
    
    private static final Logger LOG = LoggerFactory.getLogger(GcsToDatabricksSparkPipeline.class);
    
    private final SparkSession spark;
    private final SparkConfiguration config;
    
    public GcsToDatabricksSparkPipeline(SparkConfiguration config) {
        this.config = config;
        this.spark = createSparkSession();
    }
    
    /**
     * Creates and configures Spark session
     */
    private SparkSession createSparkSession() {
        SparkConf sparkConf = new SparkConf()
            .setAppName("GCS-to-Databricks-Pipeline")
            .setMaster("local[*]") // Use local[*] for local testing, or set to cluster URL for production
            .set("spark.sql.adaptive.enabled", "true")
            .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.sql.execution.arrow.pyspark.enabled", "true");
        
        return SparkSession.builder()
            .config(sparkConf)
            .getOrCreate();
    }
    
    /**
     * Main pipeline execution method
     */
    public void runPipeline() {
        try {
            LOG.info("Starting GCS to Databricks Spark pipeline");
            LOG.info("Input file: {}", config.getInputFile());
            LOG.info("Project: {}", config.getProjectId());
            LOG.info("Databricks host: {}", config.getDatabricksHost());
            
            // Step 1: Read CSV from GCS
            Dataset<Row> csvData = readCsvFromGcs();
            
            // Step 2: Transform and validate data
            Dataset<Row> transformedData = transformAndValidateData(csvData);
            
            // Step 3: Write to BigQuery as intermediate storage
            writeToBigQuery(transformedData);
            
            // Step 4: Read from BigQuery
            Dataset<Row> bigQueryData = readFromBigQuery();
            
            // Step 5: Transform for Databricks (add timestamp)
            Dataset<Row> databricksData = prepareForDatabricks(bigQueryData);
            
            // Step 6: Write to Databricks
            writeToDatabricks(databricksData);
            
            LOG.info("Pipeline execution completed successfully");
            
        } catch (Exception e) {
            LOG.error("Pipeline execution failed", e);
            throw new RuntimeException("Pipeline execution failed", e);
        } finally {
            spark.stop();
        }
    }
    
    /**
     * Reads CSV data from Google Cloud Storage
     */
    private Dataset<Row> readCsvFromGcs() {
        LOG.info("Reading CSV data from GCS: {}", config.getInputFile());
        
        // Define the schema for the CSV data
        StructType csvSchema = new StructType()
            .add("id", DataTypes.StringType, false)
            .add("product_name", DataTypes.StringType, false)
            .add("category", DataTypes.StringType, false)
            .add("price", DataTypes.DoubleType, false)
            .add("quantity", DataTypes.IntegerType, false);
        
        Dataset<Row> csvData = spark.read()
            .option("header", "true")
            .option("inferSchema", "false")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("multiline", "true")
            .schema(csvSchema)
            .csv(config.getInputFile());
        
        LOG.info("Successfully read {} rows from CSV", csvData.count());
        return csvData;
    }
    
    /**
     * Transforms and validates the data
     */
    private Dataset<Row> transformAndValidateData(Dataset<Row> inputData) {
        LOG.info("Transforming and validating data");
        
        // Clean and validate data
        Dataset<Row> cleanedData = inputData
            .filter(col("id").isNotNull().and(col("id").notEqual("")))
            .filter(col("product_name").isNotNull().and(col("product_name").notEqual("")))
            .filter(col("category").isNotNull().and(col("category").notEqual("")))
            // .filter(col("price").isNotNull().and(col("price").geq(0)))
            // .filter(col("quantity").isNotNull().and(col("quantity").geq(0)))
            .withColumn("id", trim(col("id")))
            .withColumn("product_name", trim(col("product_name")))
            .withColumn("category", trim(col("category")));
        
        // Log validation results
        long originalCount = inputData.count();
        long cleanedCount = cleanedData.count();
        long filteredCount = originalCount - cleanedCount;
        
        LOG.info("Data validation completed. Original: {}, Cleaned: {}, Filtered: {}", 
                originalCount, cleanedCount, filteredCount);
        
        if (filteredCount > 0) {
            LOG.warn("Filtered out {} rows due to validation failures", filteredCount);
        }
        
        return cleanedData;
    }
    
    /**
     * Writes data to BigQuery as intermediate storage
     */
    private void writeToBigQuery(Dataset<Row> data) {
        LOG.info("Writing data to BigQuery");
        
        try {
            // Create BigQuery client
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            
            // Define BigQuery table ID
            TableId tableId = TableId.of(config.getProjectId(), 
                                       config.getBigQueryDataset(), 
                                       config.getBigQueryTable());

            String fullTableId = config.getProjectId() + ":" + 
                     config.getBigQueryDataset() + "." + 
                     config.getBigQueryTable();
            
            // Create table schema
            Schema bigQuerySchema = Schema.of(
                Field.of("id", StandardSQLTypeName.STRING),
                Field.of("product_name", StandardSQLTypeName.STRING),
                Field.of("category", StandardSQLTypeName.STRING),
                Field.of("price", StandardSQLTypeName.FLOAT64),
                Field.of("quantity", StandardSQLTypeName.INT64)
            );
            
            // Create table if it doesn't exist
            TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(bigQuerySchema)
                .build();
            
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            // bigQuery.create(tableInfo);
            // This will create or replace the table
            // bigQuery.create(tableInfo, BigQuery.Option.of("createDisposition", "CREATE_IF_NEEDED"));
            // bigQuery.create(tableInfo, BigQuery.Option.of("writeDisposition", "WRITE_TRUNCATE"));

            LOG.info("BigQuery table created or already exists: {}", tableId);
            
            // Write data to BigQuery using Spark BigQuery connector
            data.write()
                .format("bigquery")
                // .option("table", tableId.toString())
                .option("table", fullTableId)
                .option("temporaryGcsBucket", config.getTempLocation())
                .option("writeMethod", "direct")
                .mode(SaveMode.Overwrite)
                .save();
            
            LOG.info("Successfully wrote {} rows to BigQuery", data.count());
            
        } catch (Exception e) {
            LOG.error("Failed to write to BigQuery", e);
            throw new RuntimeException("BigQuery write failed", e);
        }
    }
    
    /**
     * Reads data from BigQuery
     */
    private Dataset<Row> readFromBigQuery() {
        LOG.info("Reading data from BigQuery");
        
        String tableId = String.format("%s.%s.%s", 
            config.getProjectId(), 
            config.getBigQueryDataset(), 
            config.getBigQueryTable());
        
        Dataset<Row> bigQueryData = spark.read()
            .format("bigquery")
            .option("table", tableId)
            .load();
        
        LOG.info("Successfully read {} rows from BigQuery", bigQueryData.count());
        return bigQueryData;
    }
    
    /**
     * Prepares data for Databricks by adding timestamp
     */
    private Dataset<Row> prepareForDatabricks(Dataset<Row> data) {
        LOG.info("Preparing data for Databricks");
        
        Dataset<Row> databricksData = data
            .withColumn("processed_at", current_timestamp())
            .select(
                col("id"),
                col("product_name"),
                col("category"),
                col("price"),
                col("quantity"),
                col("processed_at")
            );

        
        LOG.info("Data prepared for Databricks with {} rows", databricksData.count());
        return databricksData;
    }
    
    /**
     * Writes data to Databricks
     */
    private void writeToDatabricks(Dataset<Row> data) {
        LOG.info("Writing data to Databricks");
        
        try {
            // Configure Databricks connection
            String jdbcUrl = createDatabricksJdbcUrl();
            
            // First, ensure the table exists with proper Databricks syntax
            createDatabricksTableIfNotExists(jdbcUrl);
            
            // Write to Databricks using a two-step approach to avoid quoted identifier issues
            try {
                writeToDatabricksWithTempTable(data, jdbcUrl);
            } catch (SQLException e) {
                LOG.error("SQL error while writing to Databricks", e);
                throw new RuntimeException("Databricks write failed due to SQL error", e);
            }
            
            LOG.info("Successfully wrote {} rows to Databricks", data.count());
            
        } catch (Exception e) {
            LOG.error("Failed to write to Databricks", e);
            throw new RuntimeException("Databricks write failed", e);
        }
    }
    
    /**
     * Creates the Databricks table if it doesn't exist using proper SQL syntax
     */
    private void createDatabricksTableIfNotExists(String jdbcUrl) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "token", config.getDatabricksToken())) {
            String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                "id STRING NOT NULL, " +
                "product_name STRING NOT NULL, " +
                "category STRING NOT NULL, " +
                "price DOUBLE NOT NULL, " +
                "quantity BIGINT NOT NULL, " +
                "processed_at TIMESTAMP NOT NULL" +
                ") USING DELTA " +
                "TBLPROPERTIES (" +
                "'delta.autoOptimize.optimizeWrite' = 'true', " +
                "'delta.autoOptimize.autoCompact' = 'true'" +
                ")",
                config.getDatabricksTable()
            );
            
            try (Statement statement = connection.createStatement()) {
                statement.execute(createTableSql);
                LOG.info("Databricks table '{}' created or already exists", config.getDatabricksTable());
            }
        } catch (SQLException e) {
            LOG.warn("Failed to create table (it might already exist): {}", e.getMessage());
            // Continue execution as the table might already exist
        }
    }
    
    /**
     * Writes data to Databricks using a temporary table approach to avoid quoted identifier issues
     */
    private void writeToDatabricksWithTempTable(Dataset<Row> data, String jdbcUrl) throws SQLException {
        String tempTableName = config.getDatabricksTable() + "_temp_" + System.currentTimeMillis();
        
        try {
            // Step 1: Create a temporary table with a simple name using direct SQL
            createTempTable(jdbcUrl, tempTableName);
            
            // Step 2: Write data to the temporary table using direct SQL inserts
            writeDataToTempTableUsingSQL(data, jdbcUrl, tempTableName);
            
            // Step 3: Copy data from temp table to main table using SQL
            copyDataFromTempTable(jdbcUrl, tempTableName, config.getDatabricksTable());
            
            // Step 4: Drop the temporary table
            dropTempTable(jdbcUrl, tempTableName);
            
            LOG.info("Successfully wrote data to Databricks using temporary table approach");
            
        } catch (SQLException e) {
            // Clean up temp table if it exists
            try {
                dropTempTable(jdbcUrl, tempTableName);
            } catch (SQLException cleanupException) {
                LOG.warn("Failed to clean up temporary table: {}", cleanupException.getMessage());
            }
            throw e;
        } catch (Exception e) {
            // Clean up temp table if it exists
            try {
                dropTempTable(jdbcUrl, tempTableName);
            } catch (SQLException cleanupException) {
                LOG.warn("Failed to clean up temporary table: {}", cleanupException.getMessage());
            }
            throw new RuntimeException("Failed to write to Databricks using temporary table approach", e);
        }
    }
    
    /**
     * Creates a temporary table with the same structure as the main table
     */
    private void createTempTable(String jdbcUrl, String tempTableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "token", config.getDatabricksToken())) {
            String createTempTableSql = String.format(
                "CREATE TABLE %s (" +
                "id STRING NOT NULL, " +
                "product_name STRING NOT NULL, " +
                "category STRING NOT NULL, " +
                "price DOUBLE NOT NULL, " +
                "quantity BIGINT NOT NULL, " +
                "processed_at TIMESTAMP NOT NULL" +
                ") USING DELTA",
                tempTableName
            );
            
            try (Statement statement = connection.createStatement()) {
                statement.execute(createTempTableSql);
                LOG.info("Created temporary table: {}", tempTableName);
            }
        }
    }
    
    /**
     * Writes data to temporary table using direct SQL inserts to avoid quoted identifier issues
     */
    private void writeDataToTempTableUsingSQL(Dataset<Row> data, String jdbcUrl, String tempTableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "token", config.getDatabricksToken())) {
            // Collect data to driver for individual inserts
            Row[] rows = (Row[]) data.collect();
            
            if (rows.length == 0) {
                LOG.info("No data to insert into temporary table");
                return;
            }
            
            // Prepare individual insert statement
            String insertSql = String.format(
                "INSERT INTO %s (id, product_name, category, price, quantity, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
                tempTableName
            );
            
            try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
                int insertedCount = 0;
                
                for (Row row : rows) {
                    statement.setString(1, row.getString(0)); // id
                    statement.setString(2, row.getString(1)); // product_name
                    statement.setString(3, row.getString(2)); // category
                    statement.setDouble(4, row.getDouble(3)); // price
                    statement.setLong(5, row.getLong(4)); // quantity
                    statement.setTimestamp(6, row.getTimestamp(5)); // processed_at
                    
                    int result = statement.executeUpdate();
                    insertedCount += result;
                }
                
                LOG.info("Inserted {} rows into temporary table {}", insertedCount, tempTableName);
            }
        }
    }
    
    /**
     * Copies data from temporary table to main table using SQL
     */
    private void copyDataFromTempTable(String jdbcUrl, String tempTableName, String mainTableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "token", config.getDatabricksToken())) {
            String copyDataSql = String.format(
                "INSERT INTO %s " +
                "SELECT id, product_name, category, price, quantity, processed_at " +
                "FROM %s",
                mainTableName,
                tempTableName
            );
            
            try (Statement statement = connection.createStatement()) {
                int rowsInserted = statement.executeUpdate(copyDataSql);
                LOG.info("Copied {} rows from {} to {}", rowsInserted, tempTableName, mainTableName);
            }
        }
    }
    
    /**
     * Drops the temporary table
     */
    private void dropTempTable(String jdbcUrl, String tempTableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "token", config.getDatabricksToken())) {
            String dropTableSql = "DROP TABLE IF EXISTS " + tempTableName;
            
            try (Statement statement = connection.createStatement()) {
                statement.execute(dropTableSql);
                LOG.info("Dropped temporary table: {}", tempTableName);
            }
        }
    }
    
    /**
     * Creates JDBC URL for Databricks connection
     */
    private String createDatabricksJdbcUrl() {
        String jdbcUrl = String.format(
            "jdbc:databricks://%s:443/%s;transportMode=http;ssl=1;httpPath=%s;AuthMech=3;UID=token;PWD=%s;UseNativeQuery=1",
            config.getDatabricksHost(),
            config.getDatabricksDatabase(),
            config.getDatabricksHttpPath(),
            config.getDatabricksToken()
        );
        
        LOG.info("Created JDBC URL for Databricks: {}", 
            jdbcUrl.replaceAll("PWD=[^;]+", "PWD=***"));
        
        return jdbcUrl;
    }
    
    /**
     * Main method for command-line execution
     */
    public static void main(String[] args) {
        if (args.length < 8) {
            System.err.println("Usage: GcsToDatabricksSparkPipeline " +
                "<projectId> <inputFile> <bigQueryDataset> <bigQueryTable> " +
                "<databricksHost> <databricksHttpPath> <databricksToken> " +
                "<databricksDatabase> <databricksTable> <tempLocation>");
            System.exit(1);
        }
        
        try {
            // Parse command line arguments
            SparkConfiguration config = new SparkConfiguration.Builder()
                .setProjectId(args[0])
                .setInputFile(args[1])
                .setBigQueryDataset(args[2])
                .setBigQueryTable(args[3])
                .setDatabricksHost(args[4])
                .setDatabricksHttpPath(args[5])
                .setDatabricksToken(args[6])
                .setDatabricksDatabase(args[7])
                .setDatabricksTable(args[8])
                .setTempLocation(args[9])
                .build();
            
            // Create and run pipeline
            GcsToDatabricksSparkPipeline pipeline = new GcsToDatabricksSparkPipeline(config);
            pipeline.runPipeline();
            
        } catch (Exception e) {
            LOG.error("Pipeline execution failed", e);
            System.exit(1);
        }
    }
}
