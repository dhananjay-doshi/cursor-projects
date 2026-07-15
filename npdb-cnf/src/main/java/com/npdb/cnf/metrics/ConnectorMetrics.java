package com.npdb.cnf.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lightweight in-process metrics for pools and queries.
 */
public final class ConnectorMetrics {

    private final ConcurrentMap<String, LongAdder> queriesByPool = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LongAdder> failuresByPool = new ConcurrentHashMap<>();
    private final LongAdder querySuccess = new LongAdder();
    private final LongAdder queryFailure = new LongAdder();
    private final LongAdder acquireTimeouts = new LongAdder();
    private final AtomicLong latencySumNs = new AtomicLong();
    private final LongAdder latencyCount = new LongAdder();
    private final ConcurrentMap<String, LongAdder> transitions = new ConcurrentHashMap<>();

    public void recordSuccess(String poolId, long latencyNs) {
        querySuccess.increment();
        queriesByPool.computeIfAbsent(poolId, k -> new LongAdder()).increment();
        latencySumNs.addAndGet(latencyNs);
        latencyCount.increment();
    }

    public void recordFailure(String poolId) {
        queryFailure.increment();
        if (poolId != null) {
            failuresByPool.computeIfAbsent(poolId, k -> new LongAdder()).increment();
        }
    }

    public void recordAcquireTimeout(String poolId) {
        acquireTimeouts.increment();
        recordFailure(poolId);
    }

    public void recordTransition(String poolId, String toState) {
        transitions.computeIfAbsent(poolId + "->" + toState, k -> new LongAdder()).increment();
    }

    public long successCount() {
        return querySuccess.sum();
    }

    public long failureCount() {
        return queryFailure.sum();
    }

    public long queriesForPool(String poolId) {
        LongAdder a = queriesByPool.get(poolId);
        return a == null ? 0L : a.sum();
    }

    public double averageLatencyMs() {
        long c = latencyCount.sum();
        if (c == 0) {
            return 0.0;
        }
        return (latencySumNs.get() / (double) c) / 1_000_000.0;
    }

    public long acquireTimeoutCount() {
        return acquireTimeouts.sum();
    }
}
