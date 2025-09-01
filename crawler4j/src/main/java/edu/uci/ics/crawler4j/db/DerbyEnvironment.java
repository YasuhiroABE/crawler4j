package edu.uci.ics.crawler4j.db;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Derby environment to replace Berkeley DB Environment
 */
public class DerbyEnvironment {
    private static final Logger logger = LoggerFactory.getLogger(DerbyEnvironment.class);

    private final String envHome;
    private final boolean transactional;
    private final boolean locking;
    private final long lockTimeout;
    private final ConcurrentHashMap<String, DerbyDatabase> databases = new ConcurrentHashMap<>();

    public DerbyEnvironment(File envHome, DerbyEnvironmentConfig config) {
        this.envHome = envHome.getAbsolutePath();
        this.transactional = config.isTransactional();
        this.locking = config.isLocking();
        this.lockTimeout = config.getLockTimeout();

        // Create environment directory if it doesn't exist
        if (!envHome.exists()) {
            if (envHome.mkdirs()) {
                logger.debug("Created Derby environment directory: " + envHome.getAbsolutePath());
            } else {
                throw new RuntimeException("Failed to create Derby environment directory: " +
                    envHome.getAbsolutePath());
            }
        }
    }

    public DerbyDatabase openDatabase(String databaseName, DerbyDatabaseConfig config) {
        return databases.computeIfAbsent(databaseName, name -> {
            return new DerbyDatabase(envHome, name, config.isTransactional(), config.isResumable());
        });
    }

    public void close() {
        // Close all databases
        for (DerbyDatabase db : databases.values()) {
            db.close();
        }
        databases.clear();

        // Shutdown Derby engine
        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (SQLException e) {
            // Expected exception when shutting down Derby
            if (!e.getMessage().contains("Derby system shutdown")) {
                logger.warn("Unexpected error during Derby shutdown: " + e.getMessage());
            }
        }
    }

    public static class DerbyEnvironmentConfig {
        private boolean allowCreate = true;
        private boolean transactional = false;
        private boolean locking = false;
        private long lockTimeout = 500; // milliseconds

        public boolean isAllowCreate() {
            return allowCreate;
        }

        public void setAllowCreate(boolean allowCreate) {
            this.allowCreate = allowCreate;
        }

        public boolean isTransactional() {
            return transactional;
        }

        public void setTransactional(boolean transactional) {
            this.transactional = transactional;
        }

        public boolean isLocking() {
            return locking;
        }

        public void setLocking(boolean locking) {
            this.locking = locking;
        }

        public long getLockTimeout() {
            return lockTimeout;
        }

        public void setLockTimeout(long lockTimeout, TimeUnit unit) {
            this.lockTimeout = unit.toMillis(lockTimeout);
        }
    }

    public static class DerbyDatabaseConfig {
        private boolean allowCreate = true;
        private boolean transactional = false;
        private boolean resumable = false;
        private boolean deferredWrite = true;

        public boolean isAllowCreate() {
            return allowCreate;
        }

        public void setAllowCreate(boolean allowCreate) {
            this.allowCreate = allowCreate;
        }

        public boolean isTransactional() {
            return transactional;
        }

        public void setTransactional(boolean transactional) {
            this.transactional = transactional;
        }

        public boolean isResumable() {
            return resumable;
        }

        public void setResumable(boolean resumable) {
            this.resumable = resumable;
        }

        public boolean isDeferredWrite() {
            return deferredWrite;
        }

        public void setDeferredWrite(boolean deferredWrite) {
            this.deferredWrite = deferredWrite;
        }
    }
}
