package com.npdb.cnf.event;

import com.npdb.cnf.status.PoolState;

import java.util.Objects;

/**
 * Event posted to the management thread via a blocking queue.
 */
public final class DbEvent {

    private final String poolId;
    private final PoolState suggestedState;
    private final DbEventSource source;
    private final String reason;
    private final long epochMs;

    public DbEvent(String poolId, PoolState suggestedState, DbEventSource source, String reason) {
        this.poolId = Objects.requireNonNull(poolId);
        this.suggestedState = Objects.requireNonNull(suggestedState);
        this.source = Objects.requireNonNull(source);
        this.reason = reason == null ? "" : reason;
        this.epochMs = System.currentTimeMillis();
    }

    public static DbEvent down(String poolId, DbEventSource source, String reason) {
        return new DbEvent(poolId, PoolState.DOWN, source, reason);
    }

    public static DbEvent up(String poolId, DbEventSource source, String reason) {
        return new DbEvent(poolId, PoolState.UP, source, reason);
    }

    public String poolId() {
        return poolId;
    }

    public PoolState suggestedState() {
        return suggestedState;
    }

    public DbEventSource source() {
        return source;
    }

    public String reason() {
        return reason;
    }

    public long epochMs() {
        return epochMs;
    }
}
