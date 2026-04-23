package x.mg.metrics.integration;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Helper for verifying metric fields in MySQL after end-to-end pipeline propagation.
 * Usage: submit job → waitForMetrics → assert* methods.
 */
class MetricsVerificationHelper {

    private final Connection conn;

    MetricsVerificationHelper(String host, int port, String database,
                              String user, String password) throws SQLException {
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database
            + "?useSSL=false&connectTimeout=5000&socketTimeout=10000";
        conn = DriverManager.getConnection(url, user, password);
    }

    /**
     * Poll until a metric row appears, returns the first matching column value.
     * E.g. waitForMetric("task_metrics", "app_id", "app_name LIKE '%SparkPi%'", 60)
     */
    String waitForMetric(String table, String selectCol, String whereClause,
                         long timeoutSec) throws Exception {
        String sql = "SELECT " + selectCol + " FROM " + table
            + " WHERE " + whereClause
            + " ORDER BY timestamp_ms DESC LIMIT 1";
        long deadline = System.currentTimeMillis() + timeoutSec * 1000;
        while (System.currentTimeMillis() < deadline) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    String val = rs.getString(1);
                    if (val != null && !val.isEmpty()) return val;
                }
            }
            Thread.sleep(5000);
        }
        throw new RuntimeException("Timed out waiting for " + selectCol + " in " + table
            + " WHERE " + whereClause + " (" + timeoutSec + "s)");
    }

    /**
     * Wait for any row to appear in the table matching the condition.
     */
    void waitForRows(String table, String whereClause, long timeoutSec) throws Exception {
        String sql = "SELECT 1 FROM " + table + " WHERE " + whereClause + " LIMIT 1";
        long deadline = System.currentTimeMillis() + timeoutSec * 1000;
        while (System.currentTimeMillis() < deadline) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) return;
            }
            Thread.sleep(5000);
        }
        throw new RuntimeException("Timed out waiting for rows in " + table
            + " WHERE " + whereClause + " (" + timeoutSec + "s)");
    }

    /**
     * Assert that dimension columns are NOT NULL and not empty string.
     */
    void assertDimensionColumns(String table, String whereClause,
                                String... columns) throws SQLException {
        String sql = "SELECT " + String.join(", ", columns)
            + " FROM " + table + " WHERE " + whereClause + " LIMIT 1";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "No rows found in " + table + " WHERE " + whereClause);
            for (String col : columns) {
                String val = rs.getString(col);
                assertNotNull(val, col + " is NULL in " + table);
                assertFalse(val.isEmpty(), col + " is empty in " + table);
            }
        }
    }

    /**
     * Assert that metric columns are non-negative (>= 0, NOT NULL).
     */
    void assertMetricColumnsNonNegative(String table, String whereClause,
                                         String... columns) throws SQLException {
        String sql = "SELECT " + String.join(", ", columns)
            + " FROM " + table + " WHERE " + whereClause + " LIMIT 1";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "No rows found in " + table + " WHERE " + whereClause);
            for (String col : columns) {
                double val = rs.getDouble(col);
                // Check wasNull after getDouble — if the column is NULL, val is 0.0 but wasNull is true
                if (rs.wasNull()) {
                    fail(col + " is NULL in " + table);
                }
                assertTrue(val >= 0, col + " is negative (" + val + ") in " + table);
            }
        }
    }

    /**
     * Assert that metric columns are strictly positive (> 0, NOT NULL).
     */
    void assertMetricColumnsPositive(String table, String whereClause,
                                      String... columns) throws SQLException {
        String sql = "SELECT " + String.join(", ", columns)
            + " FROM " + table + " WHERE " + whereClause + " LIMIT 1";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next(), "No rows found in " + table + " WHERE " + whereClause);
            for (String col : columns) {
                double val = rs.getDouble(col);
                if (rs.wasNull()) {
                    fail(col + " is NULL in " + table);
                }
                assertTrue(val > 0, col + " is not positive (" + val + ") in " + table);
            }
        }
    }

    /**
     * Assert text columns are NOT NULL and not empty.
     */
    void assertTextColumnsNotEmpty(String table, String whereClause,
                                    String... columns) throws SQLException {
        assertDimensionColumns(table, whereClause, columns);
    }

    /**
     * Get row count for a condition.
     */
    long getRowCount(String table, String whereClause) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + table + " WHERE " + whereClause;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    /**
     * Get a single string value from a query.
     */
    String getStringValue(String table, String column, String whereClause) throws SQLException {
        String sql = "SELECT " + column + " FROM " + table
            + " WHERE " + whereClause + " LIMIT 1";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) return rs.getString(1);
            return null;
        }
    }

    /**
     * Get a single double value from a query.
     */
    Double getDoubleValue(String table, String column, String whereClause) throws SQLException {
        String sql = "SELECT " + column + " FROM " + table
            + " WHERE " + whereClause + " LIMIT 1";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                double val = rs.getDouble(1);
                return rs.wasNull() ? null : val;
            }
            return null;
        }
    }

    /**
     * Expose the underlying connection for custom queries.
     */
    Connection getConnection() {
        return conn;
    }

    void close() throws SQLException {
        if (conn != null) conn.close();
    }
}
