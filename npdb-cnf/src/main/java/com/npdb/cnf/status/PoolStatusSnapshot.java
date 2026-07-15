package com.npdb.cnf.status;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable snapshot of all pool statuses published atomically.
 */
public final class PoolStatusSnapshot {

    private final List<PoolRuntimeState> pools;
    private final long version;

    public PoolStatusSnapshot(List<PoolRuntimeState> pools, long version) {
        this.pools = List.copyOf(Objects.requireNonNull(pools));
        this.version = version;
        if (this.pools.size() > 3) {
            throw new IllegalArgumentException("max 3 pools");
        }
    }

    public List<PoolRuntimeState> pools() {
        return pools;
    }

    public long version() {
        return version;
    }

    public Optional<PoolRuntimeState> find(String poolId) {
        return pools.stream().filter(p -> p.poolId().equals(poolId)).findFirst();
    }

    public int upCount() {
        int n = 0;
        for (PoolRuntimeState p : pools) {
            if (p.isUp()) {
                n++;
            }
        }
        return n;
    }

    public List<PoolRuntimeState> upPools() {
        return pools.stream().filter(PoolRuntimeState::isUp).toList();
    }

    public static PoolStatusSnapshot empty() {
        return new PoolStatusSnapshot(Collections.emptyList(), 0L);
    }
}
