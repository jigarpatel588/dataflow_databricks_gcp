# GCS to Databricks Pipeline

A Google Cloud Dataflow pipeline that demonstrates how to load CSV data from Google Cloud Storage (GCS) into Databricks using Apache Beam with Java Spark integration.

## Overview

This project provides a complete solution for:
- Reading CSV data from Google Cloud Storage
- Processing and validating data using Apache Beam
- Storing data temporarily in BigQuery for reliability
- Loading processed data into Databricks using JDBC connectivity
- Running on Apache Spark for distributed processing

## Architecture

```
GCS (CSV Files) → Apache Beam Pipeline → BigQuery (Intermediate) → Databricks
```

### Data Flow
1. **Extract**: Read CSV files from Google Cloud Storage
2. **Transform**: Parse, validate, and clean data using Apache Beam transforms
3. **Load**: Store in BigQuery as intermediate storage for reliability
4. **Transfer**: Read from BigQuery and load into Databricks via JDBC

## Prerequisites

### Software Requirements
- Java 11 or higher
- Maven 3.6 or higher
- Google Cloud SDK
- Access to Google Cloud Platform
- Databricks workspace with SQL warehouse

### GCP Setup
1. Create a Google Cloud Project
2. Enable the following APIs:
   - Cloud Storage API
   - BigQuery API
   - Dataflow API
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
│   │   ├── GcsToDatabricksPipeline.java    # Main pipeline class
│   │   ├── DatabricksConfig.java           # Databricks configuration
│   │   └── DataTransforms.java             # Data transformation utilities
│   └── test/java/com/example/              # Test classes
├── data/
│   └── products.csv                        # Sample CSV data
├── config/
│   └── application.properties              # Configuration file
├── scripts/
│   ├── create_databricks_table.sql         # SQL for table creation
│   ├── run_pipeline.sh                     # Linux/Mac runner script
│   └── run_pipeline.bat                    # Windows runner script
├── pom.xml                                 # Maven configuration
└── README.md                               # This file
```

## Configuration

### 1. Update Configuration Files

Edit `config/application.properties`:
```properties
# Google Cloud Configuration
gcp.project.id=your-gcp-project-id
gcp.temp.location=gs://your-gcp-project-id/temp
gcp.staging.location=gs://your-gcp-project-id/staging

# Databricks Configuration
databricks.host=your-databricks-host.cloud.databricks.com
databricks.http.path=/sql/1.0/warehouses/your-warehouse-id
databricks.token=your-databricks-access-token
databricks.database=default
databricks.table=products
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
mvn compile exec:java -Dexec.mainClass="com.example.GcsToDatabricksPipeline" \
  -Dexec.args="--project=your-gcp-project-id \
  --inputFile=gs://your-gcp-project-id/data/products.csv \
  --databricksHost=your-databricks-host.cloud.databricks.com \
  --databricksHttpPath=/sql/1.0/warehouses/your-warehouse-id \
  --databricksToken=your-databricks-access-token \
  --databricksDatabase=default \
  --databricksTable=products \
  --tempLocation=gs://your-gcp-project-id/temp \
  --stagingLocation=gs://your-gcp-project-id/staging \
  --runner=SparkRunner"
```

### Method 2: Using Scripts

**Linux/Mac:**
```bash
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh
```

**Windows:**
```cmd
scripts\run_pipeline.bat
```

### Method 3: Direct Runner (Local Testing)

For local testing without Spark:
```bash
mvn compile exec:java -Dexec.mainClass="com.example.GcsToDatabricksPipeline" \
  -Dexec.args="--project=your-gcp-project-id \
  --inputFile=gs://your-gcp-project-id/data/products.csv \
  --databricksHost=your-databricks-host.cloud.databricks.com \
  --databricksHttpPath=/sql/1.0/warehouses/your-warehouse-id \
  --databricksToken=your-databricks-access-token \
  --databricksDatabase=default \
  --databricksTable=products \
  --runner=DirectRunner"
```

## Data Schema

### Input CSV Format
```csv
id,product_name,category,price,quantity
P001,"Laptop Computer 15-inch",Electronics,1299.99,50
P002,"Wireless Mouse",Electronics,29.99,200
```

### BigQuery Intermediate Schema
- `id` (STRING): Product identifier
- `product_name` (STRING): Product name
- `category` (STRING): Product category
- `price` (FLOAT): Product price
- `quantity` (INTEGER): Available quantity

### Databricks Target Schema
- `id` (STRING): Product identifier
- `product_name` (STRING): Product name
- `category` (STRING): Product category
- `price` (DOUBLE): Product price
- `quantity` (INTEGER): Available quantity
- `processed_at` (TIMESTAMP): Processing timestamp

## Features

### Data Validation
- Validates CSV format and field count
- Handles quoted fields and special characters
- Validates numeric fields (price, quantity)
- Ensures non-negative values for price and quantity
- Skips malformed records with logging

### Error Handling
- Comprehensive logging throughout the pipeline
- Graceful handling of malformed data
- Retry mechanisms for network operations
- Detailed error messages for troubleshooting

### Performance Optimization
- Uses BigQuery as intermediate storage for reliability
- Parallel processing with Apache Beam
- Optimized JDBC connections to Databricks
- Efficient data transformations

## Monitoring and Logging

The pipeline provides detailed logging at each stage:
- Data reading and parsing
- Transformation operations
- BigQuery operations
- Databricks connectivity
- Error conditions and warnings

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

3. **Data Format Issues**
   - Ensure CSV follows expected format
   - Check for proper field escaping
   - Validate numeric field formats

### Debug Mode

Enable debug logging by setting:
```bash
export BEAM_LOG_LEVEL=DEBUG
```

## Dependencies

### Core Dependencies
- Apache Beam 2.50.0
- Apache Spark 3.4.1
- Google Cloud Storage 2.20.0
- Google Cloud BigQuery 2.20.0
- Databricks JDBC Driver 2.6.25

### Build Dependencies
- Maven Shade Plugin (for fat JAR creation)
- Maven Compiler Plugin (Java 11)
- Maven Surefire Plugin (testing)

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

- **v1.0.0**: Initial release with basic GCS to Databricks pipeline
- Features: CSV processing, BigQuery intermediate storage, Databricks JDBC integration
