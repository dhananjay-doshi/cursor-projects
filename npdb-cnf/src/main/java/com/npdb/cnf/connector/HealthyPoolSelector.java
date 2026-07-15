package com.npdb.cnf.connector;

import com.npdb.cnf.status.PoolRuntimeState;
import com.npdb.cnf.status.PoolStatusRegistry;
import com.npdb.cnf.status.PoolStatusSnapshot;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round-robin selection across pools currently marked UP.
 * Read-only access to {@link PoolStatusRegistry} — no locks with management writer.
 */
public final class HealthyPoolSelector {

    private final PoolStatusRegistry statusRegistry;
    private final AtomicInteger cursor = new AtomicInteger();

    public HealthyPoolSelector(PoolStatusRegistry statusRegistry) {
        this.statusRegistry = statusRegistry;
    }

    /**
     * @return next UP pool id, or null if none available
     */
    public String nextUpPoolId() {
        PoolStatusSnapshot snap = statusRegistry.current();
        List<PoolRuntimeState> up = snap.upPools();
        if (up.isEmpty()) {
            return null;
        }
        int idx = Math.floorMod(cursor.getAndIncrement(), up.size());
        return up.get(idx).poolId();
    }

    /**
     * Next UP pool after skipping {@code excludePoolId} (used by OAM retry).
     */
    public String nextUpPoolIdExcluding(String excludePoolId) {
        PoolStatusSnapshot snap = statusRegistry.current();
        List<PoolRuntimeState> up = snap.upPools().stream()
                .filter(p -> !p.poolId().equals(excludePoolId))
                .toList();
        if (up.isEmpty()) {
            return null;
        }
        int idx = Math.floorMod(cursor.getAndIncrement(), up.size());
        return up.get(idx).poolId();
    }
}
