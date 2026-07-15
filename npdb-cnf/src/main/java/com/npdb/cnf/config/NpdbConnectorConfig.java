package com.npdb.cnf.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Top-level configuration for the common DB connector.
 * PG pod count is configurable with a maximum of 3 (final HLD).
 */
public final class NpdbConnectorConfig {

    public enum ProcessType {
        DB_HANDLER,
        SERVICE_OAM
    }

    private final ProcessType processType;
    private final List<DbEndpointConfig> endpoints;
    private final PoolSettings poolSettings;
    private final long monitoringIntervalMs;
    private final int maxOamRetries;
    private final String npLookupSql;
    private final String healthSql;
    private final long bulkNetworkTimeoutMs;

    private NpdbConnectorConfig(Builder b) {
        this.processType = b.processType;
        this.endpoints = List.copyOf(b.endpoints);
        this.poolSettings = b.poolSettings;
        this.monitoringIntervalMs = b.monitoringIntervalMs;
        this.maxOamRetries = b.maxOamRetries;
        this.npLookupSql = b.npLookupSql;
        this.healthSql = b.healthSql;
        this.bulkNetworkTimeoutMs = b.bulkNetworkTimeoutMs;
        if (endpoints.isEmpty() || endpoints.size() > 3) {
            throw new IllegalArgumentException("endpoints size must be 1..3, was " + endpoints.size());
        }
    }

    public static Builder builder(ProcessType processType) {
        return new Builder(processType);
    }

    public ProcessType processType() {
        return processType;
    }

    public List<DbEndpointConfig> endpoints() {
        return endpoints;
    }

    public PoolSettings poolSettings() {
        return poolSettings;
    }

    public long monitoringIntervalMs() {
        return monitoringIntervalMs;
    }

    public int maxOamRetries() {
        return maxOamRetries;
    }

    public String npLookupSql() {
        return npLookupSql;
    }

    public String healthSql() {
        return healthSql;
    }

    public long bulkNetworkTimeoutMs() {
        return bulkNetworkTimeoutMs;
    }

    public static final class Builder {
        private final ProcessType processType;
        private final List<DbEndpointConfig> endpoints = new ArrayList<>();
        private PoolSettings poolSettings;
        private long monitoringIntervalMs = 5_000L;
        private int maxOamRetries = 2;
        private String npLookupSql =
                "SELECT subscriber_number, routing_number FROM np_subscriber WHERE subscriber_number = ?";
        private String healthSql = "SELECT 1";
        private long bulkNetworkTimeoutMs = 300_000L; // 5 minutes per final HLD

        private Builder(ProcessType processType) {
            this.processType = Objects.requireNonNull(processType);
            this.poolSettings = processType == ProcessType.DB_HANDLER
                    ? PoolSettings.dbHandlerDefaults()
                    : PoolSettings.serviceOamDefaults();
        }

        public Builder addEndpoint(DbEndpointConfig endpoint) {
            endpoints.add(endpoint);
            return this;
        }

        public Builder endpoints(List<DbEndpointConfig> list) {
            endpoints.clear();
            endpoints.addAll(list);
            return this;
        }

        public Builder poolSettings(PoolSettings poolSettings) {
            this.poolSettings = poolSettings;
            return this;
        }

        public Builder monitoringIntervalMs(long monitoringIntervalMs) {
            this.monitoringIntervalMs = monitoringIntervalMs;
            return this;
        }

        public Builder maxOamRetries(int maxOamRetries) {
            this.maxOamRetries = maxOamRetries;
            return this;
        }

        public Builder npLookupSql(String npLookupSql) {
            this.npLookupSql = npLookupSql;
            return this;
        }

        public Builder healthSql(String healthSql) {
            this.healthSql = healthSql;
            return this;
        }

        public Builder bulkNetworkTimeoutMs(long bulkNetworkTimeoutMs) {
            this.bulkNetworkTimeoutMs = bulkNetworkTimeoutMs;
            return this;
        }

        public NpdbConnectorConfig build() {
            return new NpdbConnectorConfig(this);
        }
    }
}
