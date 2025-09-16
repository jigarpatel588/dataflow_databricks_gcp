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

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;
import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * Utility class containing data transformation functions
 */
public class DataTransforms {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataTransforms.class);
    private static final String FIELD_SEPARATOR = ",";
    private static final Pattern CSV_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    
    /**
     * Transforms CSV string to TableRow for BigQuery
     */
    public static class CsvToTableRowTransform extends DoFn<String, TableRow> {
        
        private static final String[] COLUMN_NAMES = {"id", "product_name", "category", "price", "quantity"};
        
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element().trim();
            
            // Skip empty lines and comments
            if (line.isEmpty() || line.startsWith("#") || line.startsWith("//")) {
                return;
            }
            
            try {
                // Use regex to handle CSV with quoted fields
                String[] fields = CSV_PATTERN.split(line);
                
                if (fields.length < 5) {
                    LOG.warn("Skipping malformed line (insufficient fields): {}", line);
                    return;
                }
                
                // Clean and validate fields
                String id = cleanField(fields[0]);
                String productName = cleanField(fields[1]);
                String category = cleanField(fields[2]);
                double price = parseDouble(fields[3]);
                int quantity = parseInt(fields[4]);
                
                // Validate required fields
                if (id.isEmpty() || productName.isEmpty() || category.isEmpty()) {
                    LOG.warn("Skipping line with empty required fields: {}", line);
                    return;
                }
                
                TableRow row = new TableRow();
                row.set("id", id);
                row.set("product_name", productName);
                row.set("category", category);
                row.set("price", price);
                row.set("quantity", quantity);
                
                c.output(row);
                
            } catch (Exception e) {
                LOG.error("Error processing line: {} - {}", line, e.getMessage());
            }
        }
        
        private String cleanField(String field) {
            if (field == null) return "";
            return field.trim().replaceAll("^\"|\"$", ""); // Remove surrounding quotes
        }
        
        private double parseDouble(String value) {
            try {
                return Double.parseDouble(cleanField(value));
            } catch (NumberFormatException e) {
                LOG.warn("Invalid double value: {}, using 0.0", value);
                return 0.0;
            }
        }
        
        private int parseInt(String value) {
            try {
                return Integer.parseInt(cleanField(value));
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer value: {}, using 0", value);
                return 0;
            }
        }
    }
    
    /**
     * Transforms TableRow to Row for Databricks
     */
    public static class TableRowToDatabricksRowTransform extends DoFn<TableRow, Row> {
        
        private final Schema schema;
        
        public TableRowToDatabricksRowTransform() {
            this.schema = Schema.builder()
                .addStringField("id")
                .addStringField("product_name")
                .addStringField("category")
                .addDoubleField("price")
                .addInt32Field("quantity")
                .addTimestampField("processed_at")
                .build();
        }
        
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow tableRow = c.element();
            
            try {
                Row row = Row.withSchema(schema)
                    .addValue(getStringValue(tableRow, "id"))
                    .addValue(getStringValue(tableRow, "product_name"))
                    .addValue(getStringValue(tableRow, "category"))
                    .addValue(getDoubleValue(tableRow, "price"))
                    .addValue(getIntValue(tableRow, "quantity"))
                    .addValue(java.time.Instant.now())
                    .build();
                    
                c.output(row);
                
            } catch (Exception e) {
                LOG.error("Error transforming TableRow to Row: {}", e.getMessage());
            }
        }
        
        private String getStringValue(TableRow row, String field) {
            Object value = row.get(field);
            return value != null ? value.toString() : "";
        }
        
        private Double getDoubleValue(TableRow row, String field) {
            Object value = row.get(field);
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return 0.0;
        }
        
        private Integer getIntValue(TableRow row, String field) {
            Object value = row.get(field);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return 0;
        }
    }
    
    /**
     * Data validation transform
     */
    public static class DataValidationTransform extends DoFn<TableRow, TableRow> {
        
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            
            // Validate price is positive
            Object priceObj = row.get("price");
            if (priceObj instanceof Number) {
                double price = ((Number) priceObj).doubleValue();
                if (price < 0) {
                    LOG.warn("Negative price found for product {}: {}", 
                        row.get("id"), price);
                    row.set("price", 0.0);
                }
            }
            
            // Validate quantity is non-negative
            Object quantityObj = row.get("quantity");
            if (quantityObj instanceof Number) {
                int quantity = ((Number) quantityObj).intValue();
                if (quantity < 0) {
                    LOG.warn("Negative quantity found for product {}: {}", 
                        row.get("id"), quantity);
                    row.set("quantity", 0);
                }
            }
            
            c.output(row);
        }
    }
    
    /**
     * PTransform for data validation pipeline
     */
    public static class ValidateData extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
        
        @Override
        public PCollection<TableRow> expand(PCollection<TableRow> input) {
            return input.apply("Validate Data", ParDo.of(new DataValidationTransform()));
        }
    }
}
