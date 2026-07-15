package com.npdb.cnf.status;

import com.npdb.cnf.config.DbRole;

import java.util.Objects;

/**
 * Immutable per-pool status published for lock-free readers (workers / CLI).
 */
public final class PoolRuntimeState {

    private final String poolId;
    private final DbRole role;
    private final PoolState state;
    private final long lastChangedEpochMs;
    private final String lastReason;

    public PoolRuntimeState(String poolId, DbRole role, PoolState state,
                            long lastChangedEpochMs, String lastReason) {
        this.poolId = Objects.requireNonNull(poolId);
        this.role = Objects.requireNonNull(role);
        this.state = Objects.requireNonNull(state);
        this.lastChangedEpochMs = lastChangedEpochMs;
        this.lastReason = lastReason == null ? "" : lastReason;
    }

    public String poolId() {
        return poolId;
    }

    public DbRole role() {
        return role;
    }

    public PoolState state() {
        return state;
    }

    public boolean isUp() {
        return state == PoolState.UP;
    }

    public long lastChangedEpochMs() {
        return lastChangedEpochMs;
    }

    public String lastReason() {
        return lastReason;
    }

    public PoolRuntimeState withState(PoolState newState, String reason, long epochMs) {
        return new PoolRuntimeState(poolId, role, newState, epochMs, reason);
    }
}
