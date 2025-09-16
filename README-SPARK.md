# GCS to Databricks Spark Pipeline

A native Apache Spark pipeline that demonstrates how to load CSV data from Google Cloud Storage (GCS) into Databricks using Spark's native APIs with Java.

## Overview

This project provides a complete solution for:
- Reading CSV data from Google Cloud Storage using Spark
- Processing and validating data using Spark SQL and DataFrame APIs
- Storing data temporarily in BigQuery for reliability
- Loading processed data into Databricks using JDBC connectivity
- Running on Apache Spark for distributed processing

## Architecture

```
GCS (CSV Files) → Spark Pipeline → BigQuery (Intermediate) → Databricks
```

### Data Flow
1. **Extract**: Read CSV files from Google Cloud Storage using Spark
2. **Transform**: Parse, validate, and clean data using Spark SQL and DataFrame operations
3. **Load**: Store in BigQuery as intermediate storage for reliability
4. **Transfer**: Read from BigQuery and load into Databricks via JDBC

## Prerequisites

### Software Requirements
- Java 11 or higher
- Maven 3.6 or higher
- Apache Spark 3.4.1
- Google Cloud SDK
- Access to Google Cloud Platform
- Databricks workspace with SQL warehouse

### GCP Setup
1. Create a Google Cloud Project
2. Enable the following APIs:
   - Cloud Storage API
   - BigQuery API
3. Create service account with appropriate permissions
4. Set up authentication:
   ```bash
   gcloud auth application-default login
   ```

### Databricks Setup
1. Create a Databricks workspace
2. Set up a SQL warehouse
3. Generate an access token
4. Note your workspace URL and HTTP path

## Project Structure

```
gcs-to-databricks-pipeline/
├── src/
│   ├── main/java/com/example/
│   │   ├── GcsToDatabricksSparkPipeline.java    # Main Spark pipeline class
│   │   ├── SparkConfiguration.java              # Spark configuration management
│   │   └── SparkDataTransforms.java             # Spark data transformation utilities
│   └── test/java/com/example/                   # Test classes
├── data/
│   └── products.csv                             # Sample CSV data
├── config/
│   ├── application.properties                   # Original Beam configuration
│   └── spark-application.properties             # Spark configuration
├── scripts/
│   ├── create_databricks_table.sql              # SQL for table creation
│   ├── run_spark_pipeline.sh                    # Linux/Mac Spark runner script
│   └── run_spark_pipeline.bat                   # Windows Spark runner script
├── pom.xml                                      # Maven configuration
└── README-SPARK.md                              # This file
```

## Configuration

### 1. Update Configuration Files

Edit `config/spark-application.properties`:
```properties
# Google Cloud Configuration
gcp.project.id=your-gcp-project-id
gcp.input.file=gs://your-gcp-project-id/data/products.csv
gcp.temp.location=gs://your-gcp-project-id/temp

# BigQuery Configuration
bigquery.dataset=temp_dataset
bigquery.table=temp_products

# Databricks Configuration
databricks.host=your-databricks-host.cloud.databricks.com
databricks.http.path=/sql/1.0/warehouses/your-warehouse-id
databricks.token=your-databricks-access-token
databricks.database=default
databricks.table=products

# Spark Configuration
spark.master=local[*]
spark.driver.memory=2g
spark.executor.memory=2g
```

### 2. Prepare Data

Upload your CSV file to Google Cloud Storage:
```bash
gsutil cp data/products.csv gs://your-gcp-project-id/data/
```

### 3. Create Databricks Table

Run the SQL script in your Databricks workspace:
```sql
-- Execute scripts/create_databricks_table.sql
```

## Usage

### Method 1: Command Line

```bash
mvn compile exec:java -Dexec.mainClass="com.example.GcsToDatabricksSparkPipeline" \
  -Dexec.args="your-gcp-project-id \
  gs://your-gcp-project-id/data/products.csv \
  temp_dataset \
  temp_products \
  your-databricks-host.cloud.databricks.com \
  /sql/1.0/warehouses/your-warehouse-id \
  your-databricks-access-token \
  default \
  products \
  gs://your-gcp-project-id/temp"
```

### Method 2: Using Scripts

**Linux/Mac:**
```bash
chmod +x scripts/run_spark_pipeline.sh
./scripts/run_spark_pipeline.sh
```

**Windows:**
```cmd
scripts\run_spark_pipeline.bat
```

### Method 3: Using Configuration File

```java
// Load configuration from properties file
SparkConfiguration config = SparkConfiguration.fromPropertiesFile("config/spark-application.properties");

// Create and run pipeline
GcsToDatabricksSparkPipeline pipeline = new GcsToDatabricksSparkPipeline(config);
pipeline.runPipeline();
```

## Key Features

### 🔧 **Spark Native Features**
- **Spark SQL**: Advanced SQL operations and DataFrame APIs
- **Spark Core**: Distributed data processing
- **User Defined Functions (UDFs)**: Custom data transformations
- **Adaptive Query Execution**: Automatic query optimization
- **Columnar Processing**: Efficient data processing

### 🛡️ **Data Quality Features**
- **Data Validation**: Comprehensive validation rules
- **Outlier Detection**: Automatic price outlier handling
- **Data Cleaning**: Robust CSV parsing with quote handling
- **Consistency Checks**: Duplicate detection and validation
- **Quality Metrics**: Data quality scoring

### 🚀 **Performance Features**
- **Parallel Processing**: Distributed processing across multiple cores
- **Memory Optimization**: Configurable driver and executor memory
- **Adaptive Execution**: Dynamic query optimization
- **Columnar Storage**: Efficient data storage and retrieval
- **Caching**: Intelligent data caching for repeated operations

## Data Transformations

### Available Transformations

1. **Data Validation and Cleaning**
   - Remove quotes and trim whitespace
   - Validate numeric fields
   - Handle malformed records

2. **Data Quality Checks**
   - Filter invalid records
   - Detect duplicates
   - Validate business rules

3. **Data Aggregation**
   - Group by category
   - Calculate statistics
   - Generate summary reports

4. **Outlier Handling**
   - Detect price outliers
   - Cap extreme values
   - Maintain data integrity

## Testing

Run the test suite:
```bash
mvn test
```

The test suite includes:
- Configuration validation tests
- Data transformation tests
- Schema validation tests
- Data quality tests
- Integration tests

## Monitoring and Logging

The pipeline provides detailed logging at each stage:
- Data reading and parsing
- Transformation operations
- BigQuery operations
- Databricks connectivity
- Error conditions and warnings
- Performance metrics

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify GCP authentication: `gcloud auth list`
   - Check Databricks token validity
   - Ensure proper service account permissions

2. **Connection Issues**
   - Verify Databricks host and HTTP path
   - Check network connectivity
   - Validate JDBC driver availability

3. **Memory Issues**
   - Increase driver memory: `-Dspark.driver.memory=4g`
   - Increase executor memory: `-Dspark.executor.memory=4g`
   - Adjust parallelism settings

4. **Data Format Issues**
   - Ensure CSV follows expected format
   - Check for proper field escaping
   - Validate numeric field formats

### Debug Mode

Enable debug logging by setting:
```bash
export SPARK_LOG_LEVEL=DEBUG
```

## Dependencies

### Core Dependencies
- Apache Spark 3.4.1
- Google Cloud Storage 2.20.0
- Google Cloud BigQuery 2.20.0
- Spark BigQuery Connector 0.36.1
- Databricks JDBC Driver 2.6.25

### Build Dependencies
- Maven Shade Plugin (for fat JAR creation)
- Maven Compiler Plugin (Java 11)
- Maven Surefire Plugin (testing)

## Performance Tuning

### Spark Configuration
```properties
# Memory settings
spark.driver.memory=4g
spark.executor.memory=4g
spark.executor.cores=4

# Performance optimizations
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

### Data Processing
- Use appropriate partitioning strategies
- Leverage Spark's adaptive query execution
- Optimize data serialization
- Use columnar formats when possible

## Migration from Apache Beam

### Key Differences

| Feature | Apache Beam | Spark |
|---------|-------------|-------|
| **API Style** | Functional/Transform-based | DataFrame/SQL-based |
| **Execution** | Pipeline-based | RDD/DataFrame-based |
| **Transforms** | DoFn, PTransform | UDF, DataFrame operations |
| **Testing** | TestPipeline | SparkSession |
| **Configuration** | PipelineOptions | SparkConfiguration |

### Migration Benefits
- **Simpler API**: More intuitive DataFrame operations
- **Better Performance**: Native Spark optimizations
- **Rich Ecosystem**: Extensive Spark ecosystem
- **SQL Support**: Advanced SQL capabilities
- **Easier Debugging**: Better tooling and monitoring

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs for error details
3. Create an issue in the repository
4. Contact the development team

## Version History

- **v2.0.0**: Spark-based implementation
  - Features: Native Spark APIs, DataFrame operations, UDFs, advanced data quality
- **v1.0.0**: Apache Beam implementation
  - Features: CSV processing, BigQuery intermediate storage, Databricks JDBC integration
