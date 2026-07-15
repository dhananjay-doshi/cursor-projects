package com.npdb.cnf.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NpdbConnectorConfigTest {

    @Test
    void rejectsMoreThanThreeEndpoints() {
        var b = NpdbConnectorConfig.builder(NpdbConnectorConfig.ProcessType.DB_HANDLER);
        for (int i = 0; i < 4; i++) {
            b.addEndpoint(new DbEndpointConfig("p" + i, "10.0.0." + i, 5432, "db", "u", "p",
                    i == 0 ? DbRole.PRIMARY : DbRole.REPLICA));
        }
        assertThrows(IllegalArgumentException.class, b::build);
    }

    @Test
    void oamDefaultsUsePoolSizeThree() {
        NpdbConnectorConfig cfg = NpdbConnectorConfig.builder(NpdbConnectorConfig.ProcessType.SERVICE_OAM)
                .addEndpoint(new DbEndpointConfig("primary", "10.0.0.1", 5432, "db", "u", "p", DbRole.PRIMARY))
                .build();
        assertEquals(3, cfg.poolSettings().maximumPoolSize());
        assertEquals(3, cfg.poolSettings().minimumIdle());
    }

    @Test
    void handlerDefaultsUsePoolSizeSeventeen() {
        NpdbConnectorConfig cfg = NpdbConnectorConfig.builder(NpdbConnectorConfig.ProcessType.DB_HANDLER)
                .addEndpoint(new DbEndpointConfig("primary", "10.0.0.1", 5432, "db", "u", "p", DbRole.PRIMARY))
                .build();
        assertEquals(17, cfg.poolSettings().maximumPoolSize());
    }
}
