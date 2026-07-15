package com.npdb.cnf.pool;

import com.npdb.cnf.config.DbEndpointConfig;
import com.npdb.cnf.config.DbRole;
import com.zaxxer.hikari.HikariDataSource;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Holds the HikariDataSource instances keyed by pool id.
 */
public final class PoolRegistry {

    private final Map<String, ManagedPool> pools = new LinkedHashMap<>();

    public void register(DbEndpointConfig endpoint, HikariDataSource dataSource) {
        pools.put(endpoint.id(), new ManagedPool(endpoint, dataSource));
    }

    public ManagedPool require(String poolId) {
        ManagedPool p = pools.get(poolId);
        if (p == null) {
            throw new IllegalArgumentException("unknown pool: " + poolId);
        }
        return p;
    }

    public Optional<ManagedPool> findPrimary() {
        return pools.values().stream().filter(p -> p.role() == DbRole.PRIMARY).findFirst();
    }

    public Collection<ManagedPool> all() {
        return pools.values();
    }

    public void closeAll() {
        for (ManagedPool p : pools.values()) {
            p.dataSource().close();
        }
    }

    public static final class ManagedPool {
        private final DbEndpointConfig endpoint;
        private final HikariDataSource dataSource;

        public ManagedPool(DbEndpointConfig endpoint, HikariDataSource dataSource) {
            this.endpoint = Objects.requireNonNull(endpoint);
            this.dataSource = Objects.requireNonNull(dataSource);
        }

        public String id() {
            return endpoint.id();
        }

        public DbRole role() {
            return endpoint.role();
        }

        public DbEndpointConfig endpoint() {
            return endpoint;
        }

        public HikariDataSource dataSource() {
            return dataSource;
        }
    }
}
