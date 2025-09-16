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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Unit tests for the GCS to Databricks pipeline
 */
@RunWith(JUnit4.class)
public class GcsToDatabricksPipelineTest {

    @Test
    public void testCsvToTableRowTransform() {
        TestPipeline pipeline = TestPipeline.create();
        
        // Create test data
        PCollection<String> input = pipeline
            .apply(Create.of(
                "P001,\"Laptop Computer\",Electronics,1299.99,50",
                "P002,\"Wireless Mouse\",Electronics,29.99,200"
            ));
        
        // Apply transformation
        PCollection<TableRow> output = input
            .apply(ParDo.of(new DataTransforms.CsvToTableRowTransform()));
        
        // Verify output
        PAssert.that(output).satisfies(rows -> {
            int count = 0;
            for (TableRow row : rows) {
                assertNotNull("Row should not be null", row);
                assertNotNull("ID should not be null", row.get("id"));
                assertNotNull("Product name should not be null", row.get("product_name"));
                assertNotNull("Category should not be null", row.get("category"));
                assertNotNull("Price should not be null", row.get("price"));
                assertNotNull("Quantity should not be null", row.get("quantity"));
                count++;
            }
            assertEquals("Should have 2 rows", 2, count);
            return null;
        });
        
        pipeline.run();
    }
    
    @Test
    public void testCsvToTableRowTransformWithInvalidData() {
        TestPipeline pipeline = TestPipeline.create();
        
        // Create test data with invalid entries
        PCollection<String> input = pipeline
            .apply(Create.of(
                "P001,\"Laptop Computer\",Electronics,1299.99,50",  // Valid
                "",  // Empty line
                "# Comment line",  // Comment
                "P002,Electronics,29.99,200",  // Missing field
                "P003,\"Mouse\",Electronics,invalid_price,100"  // Invalid price
            ));
        
        // Apply transformation
        PCollection<TableRow> output = input
            .apply(ParDo.of(new DataTransforms.CsvToTableRowTransform()));
        
        // Should only have 1 valid row
        PAssert.that(output).satisfies(rows -> {
            int count = 0;
            for (TableRow row : rows) {
                assertNotNull("Row should not be null", row);
                count++;
            }
            assertEquals("Should have 1 valid row", 1, count);
            return null;
        });
        
        pipeline.run();
    }
    
    @Test
    public void testDatabricksConfigValidation() {
        // Test valid configuration
        DatabricksConfig validConfig = new DatabricksConfig(
            "test.databricks.com",
            "/sql/1.0/warehouses/test-warehouse",
            "test-token",
            "test_database"
        );
        assertTrue("Valid config should pass validation", validConfig.isValid());
        
        // Test invalid configurations
        DatabricksConfig invalidHost = new DatabricksConfig(
            "",
            "/sql/1.0/warehouses/test-warehouse",
            "test-token",
            "test_database"
        );
        assertFalse("Empty host should fail validation", invalidHost.isValid());
        
        DatabricksConfig invalidToken = new DatabricksConfig(
            "test.databricks.com",
            "/sql/1.0/warehouses/test-warehouse",
            "",
            "test_database"
        );
        assertFalse("Empty token should fail validation", invalidToken.isValid());
    }
    
    @Test
    public void testJdbcUrlGeneration() {
        DatabricksConfig config = new DatabricksConfig(
            "test.databricks.com",
            "/sql/1.0/warehouses/test-warehouse",
            "test-token",
            "test_database"
        );
        
        String jdbcUrl = config.createJdbcUrl();
        assertNotNull("JDBC URL should not be null", jdbcUrl);
        assertTrue("JDBC URL should contain host", jdbcUrl.contains("test.databricks.com"));
        assertTrue("JDBC URL should contain database", jdbcUrl.contains("test_database"));
        assertTrue("JDBC URL should contain HTTP path", jdbcUrl.contains("/sql/1.0/warehouses/test-warehouse"));
        assertTrue("JDBC URL should contain SSL", jdbcUrl.contains("ssl=1"));
    }
}
