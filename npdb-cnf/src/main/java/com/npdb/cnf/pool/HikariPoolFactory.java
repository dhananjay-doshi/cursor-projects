package com.npdb.cnf.pool;

import com.npdb.cnf.config.DbEndpointConfig;
import com.npdb.cnf.config.DbRole;
import com.npdb.cnf.config.PoolSettings;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Creates one HikariDataSource per Multus static IP endpoint.
 */
public final class HikariPoolFactory {

    private HikariPoolFactory() {
    }

    public static HikariDataSource create(DbEndpointConfig endpoint, PoolSettings settings, String applicationName) {
        HikariConfig cfg = new HikariConfig();
        cfg.setPoolName("npdb-" + endpoint.id());
        cfg.setJdbcUrl(buildJdbcUrl(endpoint, settings, applicationName));
        cfg.setUsername(endpoint.username());
        cfg.setPassword(endpoint.password());
        cfg.setMinimumIdle(settings.minimumIdle());
        cfg.setMaximumPoolSize(settings.maximumPoolSize());
        cfg.setConnectionTimeout(settings.connectionTimeoutMs());
        cfg.setValidationTimeout(settings.validationTimeoutMs());
        cfg.setKeepaliveTime(settings.keepaliveTimeMs());
        cfg.setMaxLifetime(settings.maxLifetimeTimeMs());
        cfg.setInitializationFailTimeout(settings.initializationFailTimeoutMs());
        cfg.setRegisterMbeans(settings.registerMbeans());
        cfg.setAutoCommit(true);
        if (endpoint.role() == DbRole.REPLICA) {
            cfg.setReadOnly(true);
        }
        return new HikariDataSource(cfg);
    }

    static String buildJdbcUrl(DbEndpointConfig endpoint, PoolSettings settings, String applicationName) {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:postgresql://")
                .append(endpoint.host())
                .append(':')
                .append(endpoint.port())
                .append('/')
                .append(endpoint.database())
                .append("?tcpKeepAlive=").append(settings.tcpKeepAlive())
                .append("&connectTimeout=").append(settings.connectTimeoutSec())
                .append("&socketTimeout=").append(settings.socketTimeoutSec())
                .append("&prepareThreshold=").append(settings.prepareThreshold())
                .append("&preparedStatementCacheQueries=").append(settings.preparedStatementCacheQueries())
                .append("&preparedStatementCacheSizeMiB=").append(settings.preparedStatementCacheSizeMiB())
                .append("&ApplicationName=").append(applicationName == null ? "npdb-cnf" : applicationName);
        return sb.toString();
    }
}
