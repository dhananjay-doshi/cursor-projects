package com.npdb.cnf.connector;

import com.npdb.cnf.config.DbEndpointConfig;
import com.npdb.cnf.config.DbRole;
import com.npdb.cnf.status.PoolState;
import com.npdb.cnf.status.PoolStatusRegistry;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HealthyPoolSelectorTest {

    @Test
    void roundRobinOnlyUsesUpPools() {
        PoolStatusRegistry reg = new PoolStatusRegistry();
        reg.initialize(List.of(
                ep("primary", DbRole.PRIMARY),
                ep("r1", DbRole.REPLICA),
                ep("r2", DbRole.REPLICA)
        ));
        reg.setState("primary", PoolState.UP, "ok");
        reg.setState("r1", PoolState.UP, "ok");
        reg.setState("r2", PoolState.DOWN, "down");

        HealthyPoolSelector selector = new HealthyPoolSelector(reg);
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            String id = selector.nextUpPoolId();
            seen.add(id);
            assertTrue(id.equals("primary") || id.equals("r1"));
        }
        assertEquals(Set.of("primary", "r1"), seen);
    }

    @Test
    void returnsNullWhenAllDown() {
        PoolStatusRegistry reg = new PoolStatusRegistry();
        reg.initialize(List.of(ep("primary", DbRole.PRIMARY)));
        HealthyPoolSelector selector = new HealthyPoolSelector(reg);
        assertNull(selector.nextUpPoolId());
    }

    private static DbEndpointConfig ep(String id, DbRole role) {
        return new DbEndpointConfig(id, "127.0.0.1", 5432, "m7np_db", "u", "p", role);
    }
}
