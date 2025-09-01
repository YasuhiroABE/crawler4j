package edu.uci.ics.crawler4j.db;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Derby database abstraction to replace Berkeley DB functionality
 */
public class DerbyDatabase {
    private static final Logger logger = LoggerFactory.getLogger(DerbyDatabase.class);

    private final String dbPath;
    private final String dbName;
    private final boolean transactional;
    private final boolean resumable;
    private Connection connection;
    private final ConcurrentHashMap<String, PreparedStatement> preparedStatements = new ConcurrentHashMap<>();
    private final AtomicInteger transactionCounter = new AtomicInteger(0);

    public DerbyDatabase(String dbPath, String dbName, boolean transactional, boolean resumable) {
        this.dbPath = dbPath;
        this.dbName = dbName;
        this.transactional = transactional;
        this.resumable = resumable;
        initializeDatabase();
    }

    private void initializeDatabase() {
        try {
            // Load Derby JDBC driver
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            } catch (ClassNotFoundException e) {
                logger.error("Derby JDBC driver not found. Make sure derby dependency is included in classpath.", e);
                throw new RuntimeException("Derby JDBC driver not found", e);
            }

            // Create database directory if it doesn't exist
            File dbDir = new File(dbPath);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }

            // Connect to Derby database
            String connectionURL = "jdbc:derby:" + dbPath + "/" + dbName + ";create=true";
            connection = DriverManager.getConnection(connectionURL);

            // Create tables if they don't exist
            createTables();

        } catch (SQLException e) {
            logger.error("Failed to initialize Derby database", e);
            throw new RuntimeException("Database initialization failed", e);
        }
    }

    private void createTables() throws SQLException {
        // Create main data table
        try (Statement stmt = connection.createStatement()) {
            try {
                stmt.execute("CREATE TABLE data_table (" +
                           "key_data VARCHAR(32672) PRIMARY KEY, " +
                           "value_data BLOB)");
            } catch (SQLException e) {
                // Table might already exist, ignore
                if (!e.getMessage().contains("already exists")) {
                    throw e;
                }
            }
        }

        // Create counters table for statistics
        try (Statement stmt = connection.createStatement()) {
            try {
                stmt.execute("CREATE TABLE counters_table (" +
                           "counter_name VARCHAR(255) PRIMARY KEY, " +
                           "counter_value BIGINT)");
            } catch (SQLException e) {
                // Table might already exist, ignore
                if (!e.getMessage().contains("already exists")) {
                    throw e;
                }
            }
        }
    }

    public DerbyTransaction beginTransaction() {
        if (transactional) {
            return new DerbyTransaction(this);
        }
        return null;
    }

    public void put(String key, byte[] value) throws SQLException {
        // First try to update, if no rows affected then insert
        String updateSql = "UPDATE data_table SET value_data = ? WHERE key_data = ?";
        PreparedStatement updateStmt = preparedStatements.computeIfAbsent(updateSql, k -> {
            try {
                return connection.prepareStatement(k);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to prepare statement", e);
            }
        });

        updateStmt.setBytes(1, value);
        updateStmt.setString(2, key);
        int rowsAffected = updateStmt.executeUpdate();

        if (rowsAffected == 0) {
            // No rows updated, so insert new record
            String insertSql = "INSERT INTO data_table (key_data, value_data) VALUES (?, ?)";
            PreparedStatement insertStmt = preparedStatements.computeIfAbsent(insertSql, k -> {
                try {
                    return connection.prepareStatement(k);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to prepare statement", e);
                }
            });

            insertStmt.setString(1, key);
            insertStmt.setBytes(2, value);
            insertStmt.executeUpdate();
        }
    }

    public byte[] get(String key) throws SQLException {
        String sql = "SELECT value_data FROM data_table WHERE key_data = ?";
        PreparedStatement pstmt = preparedStatements.computeIfAbsent(sql, k -> {
            try {
                return connection.prepareStatement(k);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to prepare statement", e);
            }
        });

        pstmt.setString(1, key);
        try (ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getBytes("value_data");
            }
        }
        return null;
    }

    public boolean contains(String key) throws SQLException {
        String sql = "SELECT 1 FROM data_table WHERE key_data = ?";
        PreparedStatement pstmt = preparedStatements.computeIfAbsent(sql, k -> {
            try {
                return connection.prepareStatement(k);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to prepare statement", e);
            }
        });

        pstmt.setString(1, key);
        try (ResultSet rs = pstmt.executeQuery()) {
            return rs.next();
        }
    }

    public void delete(String key) throws SQLException {
        String sql = "DELETE FROM data_table WHERE key_data = ?";
        PreparedStatement pstmt = preparedStatements.computeIfAbsent(sql, k -> {
            try {
                return connection.prepareStatement(k);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to prepare statement", e);
            }
        });

        pstmt.setString(1, key);
        pstmt.executeUpdate();
    }

    public long count() throws SQLException {
        String sql = "SELECT COUNT(*) FROM data_table";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

    public List<DerbyCursorEntry> getAllEntries() throws SQLException {
        List<DerbyCursorEntry> entries = new ArrayList<>();
        String sql = "SELECT key_data, value_data FROM data_table ORDER BY key_data";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String key = rs.getString("key_data");
                byte[] value = rs.getBytes("value_data");
                entries.add(new DerbyCursorEntry(key, value));
            }
        }
        return entries;
    }

    public List<DerbyCursorEntry> getFirstNEntries(int max) throws SQLException {
        List<DerbyCursorEntry> entries = new ArrayList<>();
        String sql = "SELECT key_data, value_data FROM data_table ORDER BY key_data";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next() && count < max) {
                String key = rs.getString("key_data");
                byte[] value = rs.getBytes("value_data");
                entries.add(new DerbyCursorEntry(key, value));
                count++;
            }
        }
        return entries;
    }

    public void deleteFirstNEntries(int count) throws SQLException {
        // Get the first N entries to delete
        List<DerbyCursorEntry> entries = getFirstNEntries(count);
        for (DerbyCursorEntry entry : entries) {
            delete(entry.getKey());
        }
    }

    public void setCounter(String name, long value) throws SQLException {
        // First try to update, if no rows affected then insert
        String updateSql = "UPDATE counters_table SET counter_value = ? WHERE counter_name = ?";
        PreparedStatement updateStmt = preparedStatements.computeIfAbsent(updateSql, k -> {
            try {
                return connection.prepareStatement(k);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to prepare statement", e);
            }
        });

        updateStmt.setLong(1, value);
        updateStmt.setString(2, name);
        int rowsAffected = updateStmt.executeUpdate();

        if (rowsAffected == 0) {
            // No rows updated, so insert new record
            String insertSql = "INSERT INTO counters_table (counter_name, counter_value) VALUES (?, ?)";
            PreparedStatement insertStmt = preparedStatements.computeIfAbsent(insertSql, k -> {
                try {
                    return connection.prepareStatement(k);
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to prepare statement", e);
                }
            });

            insertStmt.setString(1, name);
            insertStmt.setLong(2, value);
            insertStmt.executeUpdate();
        }
    }

    public long getCounter(String name) throws SQLException {
        String sql = "SELECT counter_value FROM counters_table WHERE counter_name = ?";
        PreparedStatement pstmt = preparedStatements.computeIfAbsent(sql, k -> {
            try {
                return connection.prepareStatement(k);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to prepare statement", e);
            }
        });

        pstmt.setString(1, name);
        try (ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong("counter_value");
            }
        }
        return 0;
    }

    public List<DerbyCursorEntry> getAllCounters() throws SQLException {
        List<DerbyCursorEntry> entries = new ArrayList<>();
        String sql = "SELECT counter_name, counter_value FROM counters_table";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String key = rs.getString("counter_name");
                long value = rs.getLong("counter_value");
                // Convert long to byte array
                byte[] valueBytes = new byte[8];
                for (int i = 0; i < 8; i++) {
                    valueBytes[i] = (byte) (value >> (8 * i));
                }
                entries.add(new DerbyCursorEntry(key, valueBytes));
            }
        }
        return entries;
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("Error closing Derby database", e);
        }
    }

    public static class DerbyCursorEntry {
        private final String key;
        private final byte[] value;

        public DerbyCursorEntry(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }

    public static class DerbyTransaction {
        private final DerbyDatabase db;
        private final Connection transactionConnection;
        private boolean committed = false;
        private boolean rolledBack = false;

        public DerbyTransaction(DerbyDatabase db) {
            this.db = db;
            try {
                this.transactionConnection = db.connection;
                transactionConnection.setAutoCommit(false);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to begin transaction", e);
            }
        }

        public void commit() {
            if (!committed && !rolledBack) {
                try {
                    transactionConnection.commit();
                    transactionConnection.setAutoCommit(true);
                    committed = true;
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to commit transaction", e);
                }
            }
        }

        public void rollback() {
            if (!committed && !rolledBack) {
                try {
                    transactionConnection.rollback();
                    transactionConnection.setAutoCommit(true);
                    rolledBack = true;
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to rollback transaction", e);
                }
            }
        }
    }
}
