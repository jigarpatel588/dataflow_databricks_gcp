# GCS to Databricks Pipeline Setup

## Configuration Setup

This project uses template configuration files to avoid committing sensitive information to version control.

### 1. Configuration Files

The following files contain template values that need to be updated with your actual credentials:

- `config/application.properties` - Main configuration file
- `scripts/run_pipeline.bat` - Windows batch script for running the pipeline

### 2. Required Configuration Values

Update the following placeholders in your configuration files:

#### Google Cloud Platform
- `YOUR_PROJECT_ID` - Your GCP project ID
- `YOUR_BUCKET` - Your GCS bucket name

#### Databricks
- `YOUR_DATABRICKS_HOST` - Your Databricks workspace host (e.g., `adb-1234567890123456.7.azuredatabricks.net`)
- `YOUR_WAREHOUSE_ID` - Your Databricks SQL warehouse ID
- `YOUR_DATABRICKS_TOKEN` - Your Databricks personal access token

### 3. Creating Local Configuration

For local development, create copies of the template files with your actual values:

```bash
# Copy and customize the configuration
cp config/application.properties config/application-local.properties
cp scripts/run_pipeline.bat scripts/run_pipeline-local.bat
```

Then update the local files with your actual credentials.

### 4. Security Notes

- Never commit files with actual credentials to version control
- The `.gitignore` file is configured to exclude local configuration files
- Use environment variables or secure credential management for production deployments

### 5. Running the Pipeline

After setting up your configuration, you can run the pipeline using:

```bash
# Windows
scripts/run_pipeline-local.bat

# Or directly with Maven
mvn exec:java -Dexec.mainClass="com.example.GcsToDatabricksSparkPipeline" -Dexec.args="..."
```

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Google Cloud SDK configured with appropriate permissions
- Databricks workspace access
- Valid GCS bucket with read permissions
- Valid BigQuery dataset with write permissions
