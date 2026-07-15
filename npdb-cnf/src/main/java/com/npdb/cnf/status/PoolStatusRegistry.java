package com.npdb.cnf.status;

import com.npdb.cnf.config.DbEndpointConfig;
import com.npdb.cnf.config.DbRole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lock-free readable pool status registry.
 * <p>
 * <b>Writers:</b> management thread only (final HLD).<br>
 * <b>Readers:</b> workers, monitoring, MnpCtl / CLI — no locks on read path.
 * <p>
 * Implementation uses {@link AtomicReference} of an immutable {@link PoolStatusSnapshot}
 * (preferred over {@code CopyOnWriteArrayList} for a consistent multi-pool view).
 * See {@code docs/POOL_STATUS_CONCURRENCY.md}.
 */
public final class PoolStatusRegistry {

    private final AtomicReference<PoolStatusSnapshot> snapshot =
            new AtomicReference<>(PoolStatusSnapshot.empty());
    private final AtomicLong version = new AtomicLong();

    public void initialize(List<DbEndpointConfig> endpoints) {
        long now = System.currentTimeMillis();
        List<PoolRuntimeState> states = new ArrayList<>(endpoints.size());
        for (DbEndpointConfig ep : endpoints) {
            // Start DOWN until monitoring/warm-up proves UP — process stays up if DB down.
            states.add(new PoolRuntimeState(ep.id(), ep.role(), PoolState.DOWN, now, "init"));
        }
        publish(states);
    }

    public PoolStatusSnapshot current() {
        return snapshot.get();
    }

    /**
     * Management-only: set one pool state and publish a new immutable snapshot.
     *
     * @return true if state changed
     */
    public boolean setState(String poolId, PoolState newState, String reason) {
        for (; ; ) {
            PoolStatusSnapshot prev = snapshot.get();
            List<PoolRuntimeState> nextList = new ArrayList<>(prev.pools().size());
            boolean changed = false;
            boolean found = false;
            long now = System.currentTimeMillis();
            for (PoolRuntimeState s : prev.pools()) {
                if (s.poolId().equals(poolId)) {
                    found = true;
                    if (s.state() == newState) {
                        nextList.add(s);
                    } else {
                        changed = true;
                        nextList.add(s.withState(newState, reason, now));
                    }
                } else {
                    nextList.add(s);
                }
            }
            if (!found) {
                throw new IllegalArgumentException("unknown poolId: " + poolId);
            }
            if (!changed) {
                return false;
            }
            PoolStatusSnapshot next = new PoolStatusSnapshot(nextList, version.incrementAndGet());
            if (snapshot.compareAndSet(prev, next)) {
                return true;
            }
        }
    }

    public OptionalPrimary primaryIfUp() {
        PoolStatusSnapshot snap = current();
        for (PoolRuntimeState s : snap.pools()) {
            if (s.role() == DbRole.PRIMARY && s.isUp()) {
                return new OptionalPrimary(s.poolId(), true);
            }
        }
        for (PoolRuntimeState s : snap.pools()) {
            if (s.role() == DbRole.PRIMARY) {
                return new OptionalPrimary(s.poolId(), false);
            }
        }
        return new OptionalPrimary(null, false);
    }

    private void publish(List<PoolRuntimeState> states) {
        snapshot.set(new PoolStatusSnapshot(states, version.incrementAndGet()));
    }

    public record OptionalPrimary(String poolId, boolean up) {
    }
}
