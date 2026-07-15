package com.npdb.cnf.pool;

import com.npdb.cnf.config.DbEndpointConfig;
import com.npdb.cnf.config.DbRole;
import com.npdb.cnf.config.PoolSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class HikariPoolFactoryTest {

    @Test
    void jdbcUrlContainsMultusHostAndTimeouts() {
        DbEndpointConfig ep = new DbEndpointConfig(
                "primary", "10.1.2.3", 5432, "m7np_db", "np", "secret", DbRole.PRIMARY);
        String url = HikariPoolFactory.buildJdbcUrl(ep, PoolSettings.dbHandlerDefaults(), "npdb-handler");
        assertTrue(url.startsWith("jdbc:postgresql://10.1.2.3:5432/m7np_db"));
        assertTrue(url.contains("tcpKeepAlive=true"));
        assertTrue(url.contains("connectTimeout=1"));
        assertTrue(url.contains("socketTimeout=1"));
        assertTrue(url.contains("ApplicationName=npdb-handler"));
    }
}
