-- SQL script to create the target table in Databricks
-- Run this script in your Databricks workspace before running the pipeline

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS default;

-- Use the default database
USE default;

-- Create the products table
CREATE TABLE IF NOT EXISTS products (
    id STRING NOT NULL,
    product_name STRING NOT NULL,
    category STRING NOT NULL,
    price DOUBLE NOT NULL,
    quantity INTEGER NOT NULL,
    processed_at TIMESTAMP NOT NULL
) 
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Create an index on the id column for better performance
CREATE INDEX IF NOT EXISTS idx_products_id ON products (id);

-- Create an index on the category column for filtering
CREATE INDEX IF NOT EXISTS idx_products_category ON products (category);

-- Insert some sample data for testing (optional)
INSERT INTO products VALUES 
('SAMPLE001', 'Sample Product 1', 'Electronics', 99.99, 10, current_timestamp()),
('SAMPLE002', 'Sample Product 2', 'Books', 19.99, 25, current_timestamp());

-- Verify the table structure
DESCRIBE products;

-- Show sample data
SELECT * FROM products LIMIT 5;
