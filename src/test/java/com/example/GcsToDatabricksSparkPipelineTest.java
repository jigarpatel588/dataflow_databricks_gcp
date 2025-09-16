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

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for the Spark-based GCS to Databricks pipeline
 */
@RunWith(JUnit4.class)
public class GcsToDatabricksSparkPipelineTest {

    private SparkSession spark;
    private SparkConfiguration config;

    @Before
    public void setUp() {
        // Create Spark session for testing
        SparkConf conf = new SparkConf()
            .setAppName("GcsToDatabricksSparkPipelineTest")
            .setMaster("local[2]")
            .set("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .set("spark.driver.host", "localhost");

        spark = SparkSession.builder()
            .config(conf)
            .getOrCreate();

        // Create test configuration
        config = new SparkConfiguration.Builder()
            .setProjectId("test-project")
            .setInputFile("gs://test-bucket/test.csv")
            .setBigQueryDataset("test_dataset")
            .setBigQueryTable("test_table")
            .setDatabricksHost("test.databricks.com")
            .setDatabricksHttpPath("/sql/1.0/warehouses/test-warehouse")
            .setDatabricksToken("test-token")
            .setDatabricksDatabase("test_database")
            .setDatabricksTable("test_table")
            .setTempLocation("gs://test-bucket/temp")
            .build();
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testSparkConfigurationValidation() {
        // Test valid configuration
        assertTrue("Valid config should pass validation", config.isValid());

        // Test invalid configurations
        SparkConfiguration invalidConfig = new SparkConfiguration.Builder()
            .setProjectId("")
            .setInputFile("gs://test-bucket/test.csv")
            .setBigQueryDataset("test_dataset")
            .setBigQueryTable("test_table")
            .setDatabricksHost("test.databricks.com")
            .setDatabricksHttpPath("/sql/1.0/warehouses/test-warehouse")
            .setDatabricksToken("test-token")
            .setDatabricksDatabase("test_database")
            .setDatabricksTable("test_table")
            .setTempLocation("gs://test-bucket/temp")
            .build();

        assertFalse("Empty project ID should fail validation", invalidConfig.isValid());
    }

    @Test
    public void testCsvSchemaCreation() {
        StructType schema = SparkDataTransforms.createCsvSchema();
        
        assertNotNull("Schema should not be null", schema);
        assertEquals("Schema should have 5 fields", 5, schema.fields().length);
        
        // Check field names and types
        assertEquals("id field should be StringType", DataTypes.StringType, schema.fields()[0].dataType());
        assertEquals("product_name field should be StringType", DataTypes.StringType, schema.fields()[1].dataType());
        assertEquals("category field should be StringType", DataTypes.StringType, schema.fields()[2].dataType());
        assertEquals("price field should be DoubleType", DataTypes.DoubleType, schema.fields()[3].dataType());
        assertEquals("quantity field should be IntegerType", DataTypes.IntegerType, schema.fields()[4].dataType());
    }

    @Test
    public void testDatabricksSchemaCreation() {
        StructType schema = SparkDataTransforms.createDatabricksSchema();
        
        assertNotNull("Schema should not be null", schema);
        assertEquals("Schema should have 6 fields", 6, schema.fields().length);
        
        // Check that it includes the processed_at timestamp field
        assertEquals("processed_at field should be TimestampType", 
            DataTypes.TimestampType, schema.fields()[5].dataType());
    }

    @Test
    public void testDataValidationAndCleaning() {
        // Create test data
        List<Row> testData = Arrays.asList(
            RowFactory.create("P001", "Laptop Computer", "Electronics", 1299.99, 50),
            RowFactory.create("P002", "Wireless Mouse", "Electronics", 29.99, 200),
            RowFactory.create("", "Empty ID Product", "Electronics", 99.99, 10), // Invalid
            RowFactory.create("P003", "", "Electronics", 199.99, 25), // Invalid
            RowFactory.create("P004", "Negative Price", "Electronics", -50.0, 30), // Invalid
            RowFactory.create("P005", "Negative Quantity", "Electronics", 99.99, -5) // Invalid
        );

        StructType schema = SparkDataTransforms.createCsvSchema();
        Dataset<Row> inputData = spark.createDataFrame(testData, schema);

        // Apply validation and cleaning
        Dataset<Row> cleanedData = SparkDataTransforms.validateAndCleanData(inputData);
        Dataset<Row> validData = SparkDataTransforms.filterValidRecords(cleanedData);

        // Should have 2 valid records
        assertEquals("Should have 2 valid records", 2, validData.count());

        // Verify the valid records
        List<Row> validRows = validData.collectAsList();
        assertEquals("First valid record ID should be P001", "P001", validRows.get(0).getString(0));
        assertEquals("Second valid record ID should be P002", "P002", validRows.get(1).getString(0));
    }

    @Test
    public void testDataTransformationForBigQuery() {
        // Create test data
        List<Row> testData = Arrays.asList(
            RowFactory.create("P001", "Laptop Computer", "Electronics", 1299.99, 50),
            RowFactory.create("P002", "Wireless Mouse", "Electronics", 29.99, 200)
        );

        StructType schema = SparkDataTransforms.createCsvSchema();
        Dataset<Row> inputData = spark.createDataFrame(testData, schema);

        // Transform for BigQuery
        Dataset<Row> bigQueryData = SparkDataTransforms.transformForBigQuery(inputData);

        // Should have same number of records
        assertEquals("Should have same number of records", 2, bigQueryData.count());
        
        // Should have 5 columns (no processed_at)
        assertEquals("Should have 5 columns", 5, bigQueryData.columns().length);
    }

    @Test
    public void testDataTransformationForDatabricks() {
        // Create test data
        List<Row> testData = Arrays.asList(
            RowFactory.create("P001", "Laptop Computer", "Electronics", 1299.99, 50),
            RowFactory.create("P002", "Wireless Mouse", "Electronics", 29.99, 200)
        );

        StructType schema = SparkDataTransforms.createCsvSchema();
        Dataset<Row> inputData = spark.createDataFrame(testData, schema);

        // Transform for Databricks
        Dataset<Row> databricksData = SparkDataTransforms.transformForDatabricks(inputData);

        // Should have same number of records
        assertEquals("Should have same number of records", 2, databricksData.count());
        
        // Should have 6 columns (including processed_at)
        assertEquals("Should have 6 columns", 6, databricksData.columns().length);
        
        // Check that processed_at column exists and is not null
        assertTrue("Should have processed_at column", 
            Arrays.asList(databricksData.columns()).contains("processed_at"));
        
        List<Row> rows = databricksData.collectAsList();
        for (Row row : rows) {
            assertNotNull("processed_at should not be null", row.get(5));
        }
    }

    @Test
    public void testDataAggregationByCategory() {
        // Create test data with multiple categories
        List<Row> testData = Arrays.asList(
            RowFactory.create("P001", "Laptop", "Electronics", 1299.99, 50),
            RowFactory.create("P002", "Mouse", "Electronics", 29.99, 200),
            RowFactory.create("P003", "Chair", "Furniture", 199.99, 25),
            RowFactory.create("P004", "Desk", "Furniture", 299.99, 15)
        );

        StructType schema = SparkDataTransforms.createCsvSchema();
        Dataset<Row> inputData = spark.createDataFrame(testData, schema);

        // Aggregate by category
        Dataset<Row> aggregatedData = SparkDataTransforms.aggregateByCategory(inputData);

        // Should have 2 categories
        assertEquals("Should have 2 categories", 2, aggregatedData.count());

        // Verify aggregation results
        List<Row> aggregatedRows = aggregatedData.collectAsList();
        for (Row row : aggregatedRows) {
            String category = row.getString(0);
            long productCount = row.getLong(1);
            long totalQuantity = row.getLong(2);
            
            if ("Electronics".equals(category)) {
                assertEquals("Electronics should have 2 products", 2, productCount);
                assertEquals("Electronics total quantity should be 250", 250, totalQuantity);
            } else if ("Furniture".equals(category)) {
                assertEquals("Furniture should have 2 products", 2, productCount);
                assertEquals("Furniture total quantity should be 40", 40, totalQuantity);
            }
        }
    }

    @Test
    public void testDataConsistencyValidation() {
        // Create test data with duplicates and invalid data
        List<Row> testData = Arrays.asList(
            RowFactory.create("P001", "Laptop", "Electronics", 1299.99, 50),
            RowFactory.create("P001", "Duplicate ID", "Electronics", 29.99, 200), // Duplicate ID
            RowFactory.create("P002", "Mouse", "Electronics", -29.99, 200), // Negative price
            RowFactory.create("P003", "Chair", "Furniture", 199.99, -25) // Negative quantity
        );

        StructType schema = SparkDataTransforms.createCsvSchema();
        Dataset<Row> inputData = spark.createDataFrame(testData, schema);

        // Data consistency validation should fail
        boolean isValid = SparkDataTransforms.validateDataConsistency(inputData);
        assertFalse("Data consistency validation should fail", isValid);
    }

    @Test
    public void testPriceOutlierHandling() {
        // Create test data with outliers
        List<Row> testData = Arrays.asList(
            RowFactory.create("P001", "Normal Product", "Electronics", 100.0, 50),
            RowFactory.create("P002", "Normal Product 2", "Electronics", 120.0, 30),
            RowFactory.create("P003", "Normal Product 3", "Electronics", 110.0, 40),
            RowFactory.create("P004", "Outlier Product", "Electronics", 10000.0, 5) // Extreme outlier
        );

        StructType schema = SparkDataTransforms.createCsvSchema();
        Dataset<Row> inputData = spark.createDataFrame(testData, schema);

        // Handle outliers
        Dataset<Row> processedData = SparkDataTransforms.handlePriceOutliers(inputData);

        // Should still have 4 records
        assertEquals("Should have 4 records", 4, processedData.count());

        // Check that extreme outlier was capped
        List<Row> rows = processedData.collectAsList();
        for (Row row : rows) {
            String id = row.getString(0);
            double price = row.getDouble(3);
            
            if ("P004".equals(id)) {
                // Price should be capped (less than original 10000.0)
                assertTrue("Outlier price should be capped", price < 10000.0);
            }
        }
    }

    @Test
    public void testDataSummaryCreation() {
        // Create test data
        List<Row> testData = Arrays.asList(
            RowFactory.create("P001", "Laptop", "Electronics", 1299.99, 50),
            RowFactory.create("P002", "Mouse", "Electronics", 29.99, 200),
            RowFactory.create("P003", "Chair", "Furniture", 199.99, 25)
        );

        StructType schema = SparkDataTransforms.createCsvSchema();
        Dataset<Row> inputData = spark.createDataFrame(testData, schema);

        // This test just ensures the method runs without exception
        // In a real test, you might capture and verify the log output
        try {
            SparkDataTransforms.createDataSummary(inputData);
            // If we get here, the method executed successfully
            assertTrue("Data summary creation should complete without exception", true);
        } catch (Exception e) {
            fail("Data summary creation should not throw exception: " + e.getMessage());
        }
    }
}
