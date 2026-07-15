package com.npdb.cnf.status;

import com.npdb.cnf.config.DbEndpointConfig;
import com.npdb.cnf.config.DbRole;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PoolStatusRegistryTest {

    @Test
    void managementWritesAreVisibleToReadersWithoutLock() {
        PoolStatusRegistry reg = new PoolStatusRegistry();
        reg.initialize(List.of(
                ep("primary", DbRole.PRIMARY),
                ep("r1", DbRole.REPLICA),
                ep("r2", DbRole.REPLICA)
        ));
        assertEquals(0, reg.current().upCount());

        assertTrue(reg.setState("r1", PoolState.UP, "probe"));
        assertEquals(1, reg.current().upCount());
        assertTrue(reg.current().find("r1").orElseThrow().isUp());

        assertFalse(reg.setState("r1", PoolState.UP, "probe-again"));
        assertTrue(reg.setState("r1", PoolState.DOWN, "fail"));
        assertEquals(0, reg.current().upCount());
    }

    private static DbEndpointConfig ep(String id, DbRole role) {
        return new DbEndpointConfig(id, "10.0.0." + id.hashCode(), 5432, "m7np_db", "u", "p", role);
    }
}
