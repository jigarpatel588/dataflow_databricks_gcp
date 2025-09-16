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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

/**
 * Apache Beam pipeline that reads CSV data from Google Cloud Storage
 * and loads it into Databricks using Spark integration.
 */
public class GcsToDatabricksPipeline {
    
    private static final Logger LOG = LoggerFactory.getLogger(GcsToDatabricksPipeline.class);
    private static final String FIELD_SEPARATOR = ",";
    
    /**
     * Pipeline options interface for command-line arguments
     */
    public interface GcsToDatabricksOptions extends PipelineOptions {
        
        @Description("Path to the input CSV file in Google Cloud Storage")
        @Validation.Required
        String getInputFile();
        void setInputFile(String value);
        
        @Description("GCP Project ID")
        @Validation.Required
        String getProject();
        void setProject(String value);
        
        @Description("BigQuery dataset for intermediate storage")
        @Default.String("temp_dataset")
        String getBigQueryDataset();
        void setBigQueryDataset(String value);
        
        @Description("BigQuery table for intermediate storage")
        @Default.String("temp_table")
        String getBigQueryTable();
        void setBigQueryTable(String value);
        
        @Description("Databricks server hostname")
        @Validation.Required
        String getDatabricksHost();
        void setDatabricksHost(String value);
        
        @Description("Databricks HTTP path")
        @Validation.Required
        String getDatabricksHttpPath();
        void setDatabricksHttpPath(String value);
        
        @Description("Databricks access token")
        @Validation.Required
        String getDatabricksToken();
        void setDatabricksToken(String value);
        
        @Description("Target Databricks database")
        @Default.String("default")
        String getDatabricksDatabase();
        void setDatabricksDatabase(String value);
        
        @Description("Target Databricks table")
        @Validation.Required
        String getDatabricksTable();
        void setDatabricksTable(String value);
        
        @Description("Temporary location for staging files")
        @Validation.Required
        String getTempLocation();
        void setTempLocation(String value);
    }
    
    /**
     * Transforms CSV string to TableRow for BigQuery intermediate storage
     */
    static class CsvToTableRowTransform extends DoFn<String, TableRow> {
        
        private static final String[] COLUMN_NAMES = {"id", "product_name", "category", "price", "quantity"};
        
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element().trim();
            if (line.isEmpty() || line.startsWith("#")) {
                return; // Skip empty lines and comments
            }
            
            String[] fields = line.split(FIELD_SEPARATOR);
            if (fields.length < 5) {
                LOG.warn("Skipping malformed line: {}", line);
                return;
            }
            
            TableRow row = new TableRow();
            row.set("id", fields[0].trim());
            row.set("product_name", fields[1].trim());
            row.set("category", fields[2].trim());
            row.set("price", Double.parseDouble(fields[3].trim()));
            row.set("quantity", Integer.parseInt(fields[4].trim()));
            
            c.output(row);
        }
    }
    
    /**
     * Transforms TableRow to Row for Databricks insertion
     */
    static class TableRowToDatabricksRowTransform extends DoFn<TableRow, Row> {
        
        private final Schema schema;
        
        public TableRowToDatabricksRowTransform() {
            this.schema = Schema.builder()
                .addStringField("id")
                .addStringField("product_name")
                .addStringField("category")
                .addDoubleField("price")
                .addInt32Field("quantity")
                .build();
        }
        
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow tableRow = c.element();
            
            Row row = Row.withSchema(schema)
                .addValue(tableRow.get("id"))
                .addValue(tableRow.get("product_name"))
                .addValue(tableRow.get("category"))
                .addValue((Double) tableRow.get("price"))
                .addValue((Integer) tableRow.get("quantity"))
                .build();
                
            c.output(row);
        }
    }
    
    /**
     * Creates BigQuery table schema
     */
    static TableSchema createBigQuerySchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("product_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("category").setType("STRING"));
        fields.add(new TableFieldSchema().setName("price").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("quantity").setType("INTEGER"));
        
        return new TableSchema().setFields(fields);
    }
    
    /**
     * Main pipeline execution method
     */
    public static void main(String[] args) {
        GcsToDatabricksOptions options = 
            PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(GcsToDatabricksOptions.class);
        
        Pipeline pipeline = Pipeline.create(options);
        
        LOG.info("Starting GCS to Databricks pipeline");
        LOG.info("Input file: {}", options.getInputFile());
        LOG.info("Project: {}", options.getProject());
        LOG.info("Databricks host: {}", options.getDatabricksHost());
        
        // Step 1: Read CSV from GCS
        PCollection<String> csvLines = pipeline
            .apply("Read CSV from GCS", TextIO.read().from(options.getInputFile()));
        
        // Step 2: Transform CSV to TableRow for BigQuery intermediate storage
        PCollection<TableRow> tableRows = csvLines
            .apply("Transform CSV to TableRow", ParDo.of(new CsvToTableRowTransform()));
        
        // Step 3: Write to BigQuery as intermediate storage
        String bigQueryTable = String.format("%s:%s.%s", 
            options.getProject(), 
            options.getBigQueryDataset(), 
            options.getBigQueryTable());
            
        tableRows.apply("Write to BigQuery", 
            BigQueryIO.writeTableRows()
                .to(bigQueryTable)
                .withSchema(createBigQuerySchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        
        // Step 4: Read from BigQuery and transform for Databricks
        PCollection<TableRow> bigQueryRows = pipeline
            .apply("Read from BigQuery", 
                BigQueryIO.readTableRows()
                    .from(bigQueryTable));
        
        // Step 5: Transform to Row format for Databricks
        PCollection<Row> databricksRows = bigQueryRows
            .apply("Transform for Databricks", ParDo.of(new TableRowToDatabricksRowTransform()));
        
        // Step 6: Write to Databricks using JDBC
        String jdbcUrl = String.format("jdbc:spark://%s:443/%s;transportMode=http;ssl=1;httpPath=%s;AuthMech=3;UID=token;PWD=%s",
            options.getDatabricksHost(),
            options.getDatabricksDatabase(),
            options.getDatabricksHttpPath(),
            options.getDatabricksToken());
        
        databricksRows.apply("Write to Databricks",
            JdbcIO.<Row>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                    "com.databricks.client.jdbc.Driver", jdbcUrl))
                .withStatement(String.format("INSERT INTO %s.%s VALUES (?, ?, ?, ?, ?)", 
                    options.getDatabricksDatabase(), 
                    options.getDatabricksTable()))
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<Row>() {
                    @Override
                    public void setParameters(Row element, java.sql.PreparedStatement statement) 
                            throws java.sql.SQLException {
                        statement.setString(1, element.getString("id"));
                        statement.setString(2, element.getString("product_name"));
                        statement.setString(3, element.getString("category"));
                        statement.setDouble(4, element.getDouble("price"));
                        statement.setInt(5, element.getInt32("quantity"));
                    }
                }));
        
        // Execute the pipeline
        LOG.info("Executing pipeline...");
        pipeline.run().waitUntilFinish();
        LOG.info("Pipeline execution completed successfully");
    }
}
