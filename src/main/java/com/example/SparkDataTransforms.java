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
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Utility class containing Spark-based data transformation functions
 */
public class SparkDataTransforms {
    
    private static final Logger LOG = LoggerFactory.getLogger(SparkDataTransforms.class);
    
    /**
     * Creates the schema for CSV data
     */
    public static StructType createCsvSchema() {
        return new StructType()
            .add("id", DataTypes.StringType, false)
            .add("product_name", DataTypes.StringType, false)
            .add("category", DataTypes.StringType, false)
            .add("price", DataTypes.DoubleType, false)
            .add("quantity", DataTypes.IntegerType, false);
    }
    
    /**
     * Creates the schema for Databricks data (includes processed_at timestamp)
     */
    public static StructType createDatabricksSchema() {
        return new StructType()
            .add("id", DataTypes.StringType, false)
            .add("product_name", DataTypes.StringType, false)
            .add("category", DataTypes.StringType, false)
            .add("price", DataTypes.DoubleType, false)
            .add("quantity", DataTypes.IntegerType, false)
            .add("processed_at", DataTypes.TimestampType, false);
    }
    
    /**
     * Validates and cleans CSV data
     */
    public static Dataset<Row> validateAndCleanData(Dataset<Row> inputData) {
        LOG.info("Validating and cleaning data");
        
        // Register UDFs for data cleaning
        SparkSession spark = inputData.sparkSession();
        
        // UDF to clean string fields (remove quotes, trim whitespace)
        spark.udf().register("cleanString", new UDF1<String, String>() {
            @Override
            public String call(String value) throws Exception {
                if (value == null) return "";
                return value.trim().replaceAll("^\"|\"$", "");
            }
        }, DataTypes.StringType);
        
        // UDF to validate and clean price
        spark.udf().register("cleanPrice", new UDF1<String, Double>() {
            @Override
            public Double call(String value) throws Exception {
                try {
                    if (value == null || value.trim().isEmpty()) return 0.0;
                    String cleaned = value.trim().replaceAll("^\"|\"$", "");
                    double price = Double.parseDouble(cleaned);
                    return price >= 0 ? price : 0.0;
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid price value: {}, using 0.0", value);
                    return 0.0;
                }
            }
        }, DataTypes.DoubleType);
        
        // UDF to validate and clean quantity
        spark.udf().register("cleanQuantity", new UDF1<String, Integer>() {
            @Override
            public Integer call(String value) throws Exception {
                try {
                    if (value == null || value.trim().isEmpty()) return 0;
                    String cleaned = value.trim().replaceAll("^\"|\"$", "");
                    int quantity = Integer.parseInt(cleaned);
                    return quantity >= 0 ? quantity : 0;
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid quantity value: {}, using 0", value);
                    return 0;
                }
            }
        }, DataTypes.IntegerType);
        
        // Apply transformations
        Dataset<Row> cleanedData = inputData
            .withColumn("id", callUDF("cleanString", col("id")))
            .withColumn("product_name", callUDF("cleanString", col("product_name")))
            .withColumn("category", callUDF("cleanString", col("category")))
            .withColumn("price", callUDF("cleanPrice", col("price")))
            .withColumn("quantity", callUDF("cleanQuantity", col("quantity")));
        
        return cleanedData;
    }
    
    /**
     * Filters out invalid records based on business rules
     */
    public static Dataset<Row> filterValidRecords(Dataset<Row> inputData) {
        LOG.info("Filtering valid records");
        
        long originalCount = inputData.count();
        
        Dataset<Row> validData = inputData
            .filter(col("id").isNotNull().and(col("id").notEqual("")))
            .filter(col("product_name").isNotNull().and(col("product_name").notEqual("")))
            .filter(col("category").isNotNull().and(col("category").notEqual("")))
            .filter(col("price").isNotNull().and(col("price").geq(0)))
            .filter(col("quantity").isNotNull().and(col("quantity").geq(0)));
        
        long validCount = validData.count();
        long filteredCount = originalCount - validCount;
        
        LOG.info("Record filtering completed. Original: {}, Valid: {}, Filtered: {}", 
                originalCount, validCount, filteredCount);
        
        if (filteredCount > 0) {
            LOG.warn("Filtered out {} invalid records", filteredCount);
        }
        
        return validData;
    }
    
    /**
     * Adds data quality metrics to the dataset
     */
    public static Dataset<Row> addDataQualityMetrics(Dataset<Row> inputData) {
        LOG.info("Adding data quality metrics");
        
        SparkSession spark = inputData.sparkSession();
        
        // UDF to calculate data quality score
        spark.udf().register("calculateQualityScore", new UDF3<String, Double, Integer, Double>() {
            @Override
            public Double call(String id, Double price, Integer quantity) throws Exception {
                double score = 1.0;
                
                // Deduct points for missing or invalid data
                if (id == null || id.trim().isEmpty()) score -= 0.2;
                if (price == null || price < 0) score -= 0.3;
                if (quantity == null || quantity < 0) score -= 0.3;
                
                return Math.max(0.0, score);
            }
        }, DataTypes.DoubleType);
        
        return inputData.withColumn("quality_score", 
callUDF("calculateQualityScore", col("id"), col("price"), col("quantity")));
    }
    
    /**
     * Transforms data for BigQuery format
     */
    public static Dataset<Row> transformForBigQuery(Dataset<Row> inputData) {
        LOG.info("Transforming data for BigQuery");
        
        return inputData.select(
col("id"),
col("product_name"),
col("category"),
col("price"),
col("quantity")
        );
    }
    
    /**
     * Transforms data for Databricks format (adds timestamp)
     */
    public static Dataset<Row> transformForDatabricks(Dataset<Row> inputData) {
        LOG.info("Transforming data for Databricks");
        
        return inputData
            .withColumn("processed_at", current_timestamp())
            .select(
col("id"),
col("product_name"),
col("category"),
col("price"),
col("quantity"),
col("processed_at")
            );
    }
    
    /**
     * Aggregates data by category
     */
    public static Dataset<Row> aggregateByCategory(Dataset<Row> inputData) {
        LOG.info("Aggregating data by category");
        
        return inputData
            .groupBy(col("category"))
            .agg(
count("*").alias("product_count"),
sum(col("quantity")).alias("total_quantity"),
avg(col("price")).alias("avg_price"),
min(col("price")).alias("min_price"),
max(col("price")).alias("max_price")
            )
            .orderBy(col("category"));
    }
    
    /**
     * Detects and handles outliers in price data
     */
    public static Dataset<Row> handlePriceOutliers(Dataset<Row> inputData) {
        LOG.info("Handling price outliers");
        
        // Calculate price statistics
        Dataset<Row> priceStats = inputData
            .agg(
avg(col("price")).alias("mean_price"),
stddev(col("price")).alias("stddev_price")
            );
        
        // Get the statistics as a single row
        List<Row> statsList = priceStats.collectAsList();
        Row stats = statsList.get(0);
        double meanPrice = stats.getDouble(0);
        double stddevPrice = stats.getDouble(1);
        
        // Define outlier threshold (3 standard deviations)
        double outlierThreshold = meanPrice + (3 * stddevPrice);
        
        LOG.info("Price statistics - Mean: {}, StdDev: {}, Outlier threshold: {}", 
                meanPrice, stddevPrice, outlierThreshold);
        
        // Filter out extreme outliers but keep the data
        Dataset<Row> filteredData = inputData
            .withColumn("is_outlier", col("price").gt(outlierThreshold))
            .withColumn("price_capped", 
when(col("price").gt(outlierThreshold), outlierThreshold)
                .otherwise(col("price")));
        
        long outlierCount = filteredData.filter(col("is_outlier")).count();
        if (outlierCount > 0) {
            LOG.warn("Found {} price outliers, capping at threshold: {}", 
                    outlierCount, outlierThreshold);
        }
        
        return filteredData.drop("is_outlier").withColumnRenamed("price_capped", "price");
    }
    
    /**
     * Validates data consistency
     */
    public static boolean validateDataConsistency(Dataset<Row> inputData) {
        LOG.info("Validating data consistency");
        
        boolean isValid = true;
        
        // Check for null values in required fields
        long nullIdCount = inputData.filter(col("id").isNull()).count();
        if (nullIdCount > 0) {
            LOG.error("Found {} records with null ID", nullIdCount);
            isValid = false;
        }
        
        // Check for duplicate IDs
        long totalCount = inputData.count();
        long uniqueIdCount = inputData.select("id").distinct().count();
        if (totalCount != uniqueIdCount) {
            LOG.error("Found duplicate IDs. Total: {}, Unique: {}", totalCount, uniqueIdCount);
            isValid = false;
        }
        
        // Check for negative prices
        long negativePriceCount = inputData.filter(col("price").lt(0)).count();
        if (negativePriceCount > 0) {
            LOG.error("Found {} records with negative prices", negativePriceCount);
            isValid = false;
        }
        
        // Check for negative quantities
        long negativeQuantityCount = inputData.filter(col("quantity").lt(0)).count();
        if (negativeQuantityCount > 0) {
            LOG.error("Found {} records with negative quantities", negativeQuantityCount);
            isValid = false;
        }
        
        if (isValid) {
            LOG.info("Data consistency validation passed");
        } else {
            LOG.error("Data consistency validation failed");
        }
        
        return isValid;
    }
    
    /**
     * Creates a summary report of the dataset
     */
    public static void createDataSummary(Dataset<Row> inputData) {
        LOG.info("Creating data summary report");
        
        long totalRecords = inputData.count();
        long uniqueCategories = inputData.select("category").distinct().count();
        
        Dataset<Row> priceStats = inputData.agg(
min(col("price")).alias("min_price"),
max(col("price")).alias("max_price"),
avg(col("price")).alias("avg_price"),
sum(col("price")).alias("total_price")
        );
        
        Dataset<Row> quantityStats = inputData.agg(
min(col("quantity")).alias("min_quantity"),
max(col("quantity")).alias("max_quantity"),
avg(col("quantity")).alias("avg_quantity"),
sum(col("quantity")).alias("total_quantity")
        );
        
        List<Row> priceList = priceStats.collectAsList();
        List<Row> quantityList = quantityStats.collectAsList();
        Row priceRow = priceList.get(0);
        Row quantityRow = quantityList.get(0);
        
        LOG.info("=== DATA SUMMARY ===");
        LOG.info("Total Records: {}", totalRecords);
        LOG.info("Unique Categories: {}", uniqueCategories);
        LOG.info("Price Range: {} - {}", priceRow.getDouble(0), priceRow.getDouble(1));
        LOG.info("Average Price: {}", priceRow.getDouble(2));
        LOG.info("Total Price Value: {}", priceRow.getDouble(3));
        LOG.info("Quantity Range: {} - {}", quantityRow.getInt(0), quantityRow.getInt(1));
        LOG.info("Average Quantity: {}", quantityRow.getDouble(2));
        LOG.info("Total Quantity: {}", quantityRow.getLong(3));
        LOG.info("==================");
    }
}
