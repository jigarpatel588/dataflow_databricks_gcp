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

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for Databricks connection parameters
 */
public class DatabricksConfig {
    
    private static final Logger LOG = LoggerFactory.getLogger(DatabricksConfig.class);
    
    private final String host;
    private final String httpPath;
    private final String token;
    private final String database;
    private final int port;
    private final boolean useSSL;
    
    public DatabricksConfig(String host, String httpPath, String token, String database) {
        this(host, httpPath, token, database, 443, true);
    }
    
    public DatabricksConfig(String host, String httpPath, String token, String database, 
                           int port, boolean useSSL) {
        this.host = host;
        this.httpPath = httpPath;
        this.token = token;
        this.database = database;
        this.port = port;
        this.useSSL = useSSL;
    }
    
    /**
     * Creates JDBC URL for Databricks connection
     */
    public String createJdbcUrl() {
        String protocol = useSSL ? "https" : "http";
        String sslParam = useSSL ? "ssl=1" : "ssl=0";
        
        String jdbcUrl = String.format(
            "jdbc:spark://%s:%d/%s;transportMode=http;%s;httpPath=%s;AuthMech=3;UID=token;PWD=%s",
            host, port, database, sslParam, httpPath, token
        );
        
        LOG.info("Created JDBC URL for Databricks: {}", 
            jdbcUrl.replaceAll("PWD=[^;]+", "PWD=***"));
        
        return jdbcUrl;
    }
    
    /**
     * Creates JDBC DataSource configuration for Apache Beam
     */
    public JdbcIO.DataSourceConfiguration createDataSourceConfig() {
        return JdbcIO.DataSourceConfiguration.create(
            "com.databricks.client.jdbc.Driver",
            createJdbcUrl()
        );
    }
    
    /**
     * Validates the configuration parameters
     */
    public boolean isValid() {
        if (host == null || host.trim().isEmpty()) {
            LOG.error("Databricks host is required");
            return false;
        }
        
        if (httpPath == null || httpPath.trim().isEmpty()) {
            LOG.error("Databricks HTTP path is required");
            return false;
        }
        
        if (token == null || token.trim().isEmpty()) {
            LOG.error("Databricks access token is required");
            return false;
        }
        
        if (database == null || database.trim().isEmpty()) {
            LOG.error("Databricks database is required");
            return false;
        }
        
        if (port <= 0 || port > 65535) {
            LOG.error("Invalid port number: {}", port);
            return false;
        }
        
        return true;
    }
    
    // Getters
    public String getHost() { return host; }
    public String getHttpPath() { return httpPath; }
    public String getToken() { return token; }
    public String getDatabase() { return database; }
    public int getPort() { return port; }
    public boolean isUseSSL() { return useSSL; }
    
    @Override
    public String toString() {
        return String.format("DatabricksConfig{host='%s', httpPath='%s', database='%s', port=%d, useSSL=%s}", 
            host, httpPath, database, port, useSSL);
    }
}
