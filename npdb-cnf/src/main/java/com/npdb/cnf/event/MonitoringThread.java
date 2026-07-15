package com.npdb.cnf.event;

import com.npdb.cnf.pool.PoolRegistry;
import com.npdb.cnf.status.PoolRuntimeState;
import com.npdb.cnf.status.PoolState;
import com.npdb.cnf.status.PoolStatusRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Probes each Hikari pool every monitoringInterval (default 5s) with SELECT 1.
 * Reports UP/DOWN suggestions to the management thread; does not mutate status itself.
 */
public final class MonitoringThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MonitoringThread.class);

    private final PoolRegistry poolRegistry;
    private final PoolStatusRegistry statusRegistry;
    private final BlockingQueue<DbEvent> eventQueue;
    private final String healthSql;
    private final long intervalMs;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public MonitoringThread(PoolRegistry poolRegistry,
                            PoolStatusRegistry statusRegistry,
                            BlockingQueue<DbEvent> eventQueue,
                            String healthSql,
                            long intervalMs) {
        this.poolRegistry = poolRegistry;
        this.statusRegistry = statusRegistry;
        this.eventQueue = eventQueue;
        this.healthSql = healthSql;
        this.intervalMs = intervalMs;
    }

    public void requestStop() {
        running.set(false);
    }

    @Override
    public void run() {
        log.info("Monitoring thread started, interval={}ms", intervalMs);
        while (running.get()) {
            try {
                probeAll();
                Thread.sleep(intervalMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("Monitoring cycle error: {}", e.toString());
            }
        }
        log.info("Monitoring thread stopped");
    }

    private void probeAll() {
        for (PoolRegistry.ManagedPool pool : poolRegistry.all()) {
            boolean ok = probe(pool);
            PoolRuntimeState current = statusRegistry.current().find(pool.id()).orElse(null);
            PoolState now = current == null ? PoolState.DOWN : current.state();
            if (ok && now == PoolState.DOWN) {
                offer(DbEvent.up(pool.id(), DbEventSource.MONITORING, "health-check-ok"));
            } else if (!ok && now == PoolState.UP) {
                offer(DbEvent.down(pool.id(), DbEventSource.MONITORING, "health-check-failed"));
            }
        }
    }

    private boolean probe(PoolRegistry.ManagedPool pool) {
        try (Connection c = pool.dataSource().getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(healthSql)) {
            return rs.next();
        } catch (Exception e) {
            log.debug("Probe failed for {}: {}", pool.id(), e.toString());
            return false;
        }
    }

    private void offer(DbEvent event) {
        if (!eventQueue.offer(event)) {
            log.warn("DB event queue full, dropping {}", event.suggestedState());
        }
    }
}
