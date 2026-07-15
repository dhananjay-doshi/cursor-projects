package com.npdb.cnf.config;

/**
 * HikariCP + JDBC settings for a process profile (DB Handler vs Service OAM).
 */
public final class PoolSettings {

    private final int minimumIdle;
    private final int maximumPoolSize;
    private final long connectionTimeoutMs;
    private final long validationTimeoutMs;
    private final long keepaliveTimeMs;
    private final long maxLifetimeTimeMs;
    private final long initializationFailTimeoutMs;
    private final boolean registerMbeans;
    private final int connectTimeoutSec;
    private final int socketTimeoutSec;
    private final boolean tcpKeepAlive;
    private final int prepareThreshold;
    private final int preparedStatementCacheQueries;
    private final int preparedStatementCacheSizeMiB;
    private final int statementQueryTimeoutSec;

    private PoolSettings(Builder b) {
        this.minimumIdle = b.minimumIdle;
        this.maximumPoolSize = b.maximumPoolSize;
        this.connectionTimeoutMs = b.connectionTimeoutMs;
        this.validationTimeoutMs = b.validationTimeoutMs;
        this.keepaliveTimeMs = b.keepaliveTimeMs;
        this.maxLifetimeTimeMs = b.maxLifetimeTimeMs;
        this.initializationFailTimeoutMs = b.initializationFailTimeoutMs;
        this.registerMbeans = b.registerMbeans;
        this.connectTimeoutSec = b.connectTimeoutSec;
        this.socketTimeoutSec = b.socketTimeoutSec;
        this.tcpKeepAlive = b.tcpKeepAlive;
        this.prepareThreshold = b.prepareThreshold;
        this.preparedStatementCacheQueries = b.preparedStatementCacheQueries;
        this.preparedStatementCacheSizeMiB = b.preparedStatementCacheSizeMiB;
        this.statementQueryTimeoutSec = b.statementQueryTimeoutSec;
    }

    /** Finalized HLD defaults for DB Handler (17/17 per pool). */
    public static PoolSettings dbHandlerDefaults() {
        return builder()
                .minimumIdle(17)
                .maximumPoolSize(17)
                .connectionTimeoutMs(1000)
                .validationTimeoutMs(250)
                .keepaliveTimeMs(30_000)
                .maxLifetimeTimeMs(1_800_000)
                .initializationFailTimeoutMs(0)
                .registerMbeans(true)
                .connectTimeoutSec(1)
                .socketTimeoutSec(1)
                .tcpKeepAlive(true)
                .prepareThreshold(3)
                .preparedStatementCacheQueries(20)
                .preparedStatementCacheSizeMiB(5)
                .statementQueryTimeoutSec(2)
                .build();
    }

    /** Finalized HLD defaults for Service OAM (3/3 per pool). */
    public static PoolSettings serviceOamDefaults() {
        return builder()
                .minimumIdle(3)
                .maximumPoolSize(3)
                .connectionTimeoutMs(1000)
                .validationTimeoutMs(250)
                .keepaliveTimeMs(30_000)
                .maxLifetimeTimeMs(1_800_000)
                .initializationFailTimeoutMs(0)
                .registerMbeans(true)
                .connectTimeoutSec(1)
                .socketTimeoutSec(1)
                .tcpKeepAlive(true)
                .prepareThreshold(3)
                .preparedStatementCacheQueries(20)
                .preparedStatementCacheSizeMiB(5)
                .statementQueryTimeoutSec(2)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int minimumIdle() {
        return minimumIdle;
    }

    public int maximumPoolSize() {
        return maximumPoolSize;
    }

    public long connectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public long validationTimeoutMs() {
        return validationTimeoutMs;
    }

    public long keepaliveTimeMs() {
        return keepaliveTimeMs;
    }

    public long maxLifetimeTimeMs() {
        return maxLifetimeTimeMs;
    }

    public long initializationFailTimeoutMs() {
        return initializationFailTimeoutMs;
    }

    public boolean registerMbeans() {
        return registerMbeans;
    }

    public int connectTimeoutSec() {
        return connectTimeoutSec;
    }

    public int socketTimeoutSec() {
        return socketTimeoutSec;
    }

    public boolean tcpKeepAlive() {
        return tcpKeepAlive;
    }

    public int prepareThreshold() {
        return prepareThreshold;
    }

    public int preparedStatementCacheQueries() {
        return preparedStatementCacheQueries;
    }

    public int preparedStatementCacheSizeMiB() {
        return preparedStatementCacheSizeMiB;
    }

    public int statementQueryTimeoutSec() {
        return statementQueryTimeoutSec;
    }

    public static final class Builder {
        private int minimumIdle = 17;
        private int maximumPoolSize = 17;
        private long connectionTimeoutMs = 1000;
        private long validationTimeoutMs = 250;
        private long keepaliveTimeMs = 30_000;
        private long maxLifetimeTimeMs = 1_800_000;
        private long initializationFailTimeoutMs = 0;
        private boolean registerMbeans = true;
        private int connectTimeoutSec = 1;
        private int socketTimeoutSec = 1;
        private boolean tcpKeepAlive = true;
        private int prepareThreshold = 3;
        private int preparedStatementCacheQueries = 20;
        private int preparedStatementCacheSizeMiB = 5;
        private int statementQueryTimeoutSec = 2;

        public Builder minimumIdle(int v) {
            this.minimumIdle = v;
            return this;
        }

        public Builder maximumPoolSize(int v) {
            this.maximumPoolSize = v;
            return this;
        }

        public Builder connectionTimeoutMs(long v) {
            this.connectionTimeoutMs = v;
            return this;
        }

        public Builder validationTimeoutMs(long v) {
            this.validationTimeoutMs = v;
            return this;
        }

        public Builder keepaliveTimeMs(long v) {
            this.keepaliveTimeMs = v;
            return this;
        }

        public Builder maxLifetimeTimeMs(long v) {
            this.maxLifetimeTimeMs = v;
            return this;
        }

        public Builder initializationFailTimeoutMs(long v) {
            this.initializationFailTimeoutMs = v;
            return this;
        }

        public Builder registerMbeans(boolean v) {
            this.registerMbeans = v;
            return this;
        }

        public Builder connectTimeoutSec(int v) {
            this.connectTimeoutSec = v;
            return this;
        }

        public Builder socketTimeoutSec(int v) {
            this.socketTimeoutSec = v;
            return this;
        }

        public Builder tcpKeepAlive(boolean v) {
            this.tcpKeepAlive = v;
            return this;
        }

        public Builder prepareThreshold(int v) {
            this.prepareThreshold = v;
            return this;
        }

        public Builder preparedStatementCacheQueries(int v) {
            this.preparedStatementCacheQueries = v;
            return this;
        }

        public Builder preparedStatementCacheSizeMiB(int v) {
            this.preparedStatementCacheSizeMiB = v;
            return this;
        }

        public Builder statementQueryTimeoutSec(int v) {
            this.statementQueryTimeoutSec = v;
            return this;
        }

        public PoolSettings build() {
            if (minimumIdle > maximumPoolSize) {
                throw new IllegalArgumentException("minimumIdle cannot exceed maximumPoolSize");
            }
            return new PoolSettings(this);
        }
    }
}
