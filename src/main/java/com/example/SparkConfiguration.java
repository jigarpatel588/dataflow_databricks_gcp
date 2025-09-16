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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Configuration class for Spark-based GCS to Databricks pipeline
 */
public class SparkConfiguration {
    
    private static final Logger LOG = LoggerFactory.getLogger(SparkConfiguration.class);
    
    // GCP Configuration
    private final String projectId;
    private final String inputFile;
    private final String tempLocation;
    private final String stagingLocation;
    
    // BigQuery Configuration
    private final String bigQueryDataset;
    private final String bigQueryTable;
    
    // Databricks Configuration
    private final String databricksHost;
    private final String databricksHttpPath;
    private final String databricksToken;
    private final String databricksDatabase;
    private final String databricksTable;
    
    // Pipeline Configuration
    private final String jobName;
    private final int parallelism;
    private final String loggingLevel;
    
    private SparkConfiguration(Builder builder) {
        this.projectId = builder.projectId;
        this.inputFile = builder.inputFile;
        this.tempLocation = builder.tempLocation;
        this.stagingLocation = builder.stagingLocation;
        this.bigQueryDataset = builder.bigQueryDataset;
        this.bigQueryTable = builder.bigQueryTable;
        this.databricksHost = builder.databricksHost;
        this.databricksHttpPath = builder.databricksHttpPath;
        this.databricksToken = builder.databricksToken;
        this.databricksDatabase = builder.databricksDatabase;
        this.databricksTable = builder.databricksTable;
        this.jobName = builder.jobName;
        this.parallelism = builder.parallelism;
        this.loggingLevel = builder.loggingLevel;
    }
    
    /**
     * Creates configuration from properties file
     */
    public static SparkConfiguration fromPropertiesFile(String propertiesFile) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
            props.load(fis);
        } catch (IOException e) {
            LOG.error("Failed to load properties file: {}", propertiesFile, e);
            throw new RuntimeException("Failed to load configuration", e);
        }
        
        return new Builder()
            .setProjectId(props.getProperty("gcp.project.id"))
            .setInputFile(props.getProperty("gcp.input.file"))
            .setTempLocation(props.getProperty("gcp.temp.location"))
            .setStagingLocation(props.getProperty("gcp.staging.location"))
            .setBigQueryDataset(props.getProperty("bigquery.dataset"))
            .setBigQueryTable(props.getProperty("bigquery.table"))
            .setDatabricksHost(props.getProperty("databricks.host"))
            .setDatabricksHttpPath(props.getProperty("databricks.http.path"))
            .setDatabricksToken(props.getProperty("databricks.token"))
            .setDatabricksDatabase(props.getProperty("databricks.database"))
            .setDatabricksTable(props.getProperty("databricks.table"))
            .setJobName(props.getProperty("pipeline.job.name", "gcs-to-databricks-spark-pipeline"))
            .setParallelism(Integer.parseInt(props.getProperty("pipeline.parallelism", "4")))
            .setLoggingLevel(props.getProperty("logging.level", "INFO"))
            .build();
    }
    
    /**
     * Validates the configuration
     */
    public boolean isValid() {
        if (projectId == null || projectId.trim().isEmpty()) {
            LOG.error("GCP project ID is required");
            return false;
        }
        
        if (inputFile == null || inputFile.trim().isEmpty()) {
            LOG.error("Input file path is required");
            return false;
        }
        
        if (tempLocation == null || tempLocation.trim().isEmpty()) {
            LOG.error("Temp location is required");
            return false;
        }
        
        if (bigQueryDataset == null || bigQueryDataset.trim().isEmpty()) {
            LOG.error("BigQuery dataset is required");
            return false;
        }
        
        if (bigQueryTable == null || bigQueryTable.trim().isEmpty()) {
            LOG.error("BigQuery table is required");
            return false;
        }
        
        if (databricksHost == null || databricksHost.trim().isEmpty()) {
            LOG.error("Databricks host is required");
            return false;
        }
        
        if (databricksHttpPath == null || databricksHttpPath.trim().isEmpty()) {
            LOG.error("Databricks HTTP path is required");
            return false;
        }
        
        if (databricksToken == null || databricksToken.trim().isEmpty()) {
            LOG.error("Databricks access token is required");
            return false;
        }
        
        if (databricksDatabase == null || databricksDatabase.trim().isEmpty()) {
            LOG.error("Databricks database is required");
            return false;
        }
        
        if (databricksTable == null || databricksTable.trim().isEmpty()) {
            LOG.error("Databricks table is required");
            return false;
        }
        
        if (parallelism <= 0) {
            LOG.error("Parallelism must be greater than 0");
            return false;
        }
        
        return true;
    }
    
    // Getters
    public String getProjectId() { return projectId; }
    public String getInputFile() { return inputFile; }
    public String getTempLocation() { return tempLocation; }
    public String getStagingLocation() { return stagingLocation; }
    public String getBigQueryDataset() { return bigQueryDataset; }
    public String getBigQueryTable() { return bigQueryTable; }
    public String getDatabricksHost() { return databricksHost; }
    public String getDatabricksHttpPath() { return databricksHttpPath; }
    public String getDatabricksToken() { return databricksToken; }
    public String getDatabricksDatabase() { return databricksDatabase; }
    public String getDatabricksTable() { return databricksTable; }
    public String getJobName() { return jobName; }
    public int getParallelism() { return parallelism; }
    public String getLoggingLevel() { return loggingLevel; }
    
    @Override
    public String toString() {
        return String.format("SparkConfiguration{" +
            "projectId='%s', " +
            "inputFile='%s', " +
            "bigQueryDataset='%s', " +
            "bigQueryTable='%s', " +
            "databricksHost='%s', " +
            "databricksDatabase='%s', " +
            "databricksTable='%s', " +
            "jobName='%s', " +
            "parallelism=%d, " +
            "loggingLevel='%s'" +
            "}", 
            projectId, inputFile, bigQueryDataset, bigQueryTable,
            databricksHost, databricksDatabase, databricksTable,
            jobName, parallelism, loggingLevel);
    }
    
    /**
     * Builder class for SparkConfiguration
     */
    public static class Builder {
        private String projectId;
        private String inputFile;
        private String tempLocation;
        private String stagingLocation;
        private String bigQueryDataset;
        private String bigQueryTable;
        private String databricksHost;
        private String databricksHttpPath;
        private String databricksToken;
        private String databricksDatabase;
        private String databricksTable;
        private String jobName = "gcs-to-databricks-spark-pipeline";
        private int parallelism = 4;
        private String loggingLevel = "INFO";
        
        public Builder setProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }
        
        public Builder setInputFile(String inputFile) {
            this.inputFile = inputFile;
            return this;
        }
        
        public Builder setTempLocation(String tempLocation) {
            this.tempLocation = tempLocation;
            return this;
        }
        
        public Builder setStagingLocation(String stagingLocation) {
            this.stagingLocation = stagingLocation;
            return this;
        }
        
        public Builder setBigQueryDataset(String bigQueryDataset) {
            this.bigQueryDataset = bigQueryDataset;
            return this;
        }
        
        public Builder setBigQueryTable(String bigQueryTable) {
            this.bigQueryTable = bigQueryTable;
            return this;
        }
        
        public Builder setDatabricksHost(String databricksHost) {
            this.databricksHost = databricksHost;
            return this;
        }
        
        public Builder setDatabricksHttpPath(String databricksHttpPath) {
            this.databricksHttpPath = databricksHttpPath;
            return this;
        }
        
        public Builder setDatabricksToken(String databricksToken) {
            this.databricksToken = databricksToken;
            return this;
        }
        
        public Builder setDatabricksDatabase(String databricksDatabase) {
            this.databricksDatabase = databricksDatabase;
            return this;
        }
        
        public Builder setDatabricksTable(String databricksTable) {
            this.databricksTable = databricksTable;
            return this;
        }
        
        public Builder setJobName(String jobName) {
            this.jobName = jobName;
            return this;
        }
        
        public Builder setParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }
        
        public Builder setLoggingLevel(String loggingLevel) {
            this.loggingLevel = loggingLevel;
            return this;
        }
        
        public SparkConfiguration build() {
            return new SparkConfiguration(this);
        }
    }
}
