package com.npdb.cnf.event;

import com.npdb.cnf.metrics.ConnectorMetrics;
import com.npdb.cnf.spi.NoOpStatusListener;
import com.npdb.cnf.spi.StatusListener;
import com.npdb.cnf.status.PoolState;
import com.npdb.cnf.status.PoolStatusRegistry;
import com.npdb.cnf.status.PoolStatusSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sole writer of authoritative pool status (final HLD).
 * Consumes {@link DbEvent}s, updates {@link PoolStatusRegistry}, raises alarms, broadcasts.
 */
public final class ManagementThread implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ManagementThread.class);

    private final BlockingQueue<DbEvent> eventQueue;
    private final PoolStatusRegistry statusRegistry;
    private final ConnectorMetrics metrics;
    private final AtomicReference<StatusListener> listener =
            new AtomicReference<>(NoOpStatusListener.INSTANCE);
    private final AtomicBoolean running = new AtomicBoolean(true);

    public ManagementThread(BlockingQueue<DbEvent> eventQueue,
                            PoolStatusRegistry statusRegistry,
                            ConnectorMetrics metrics) {
        this.eventQueue = eventQueue;
        this.statusRegistry = statusRegistry;
        this.metrics = metrics;
    }

    public void setStatusListener(StatusListener statusListener) {
        listener.set(statusListener == null ? NoOpStatusListener.INSTANCE : statusListener);
    }

    public void requestStop() {
        running.set(false);
    }

    @Override
    public void run() {
        log.info("Management thread started");
        while (running.get()) {
            try {
                DbEvent event = eventQueue.take();
                handle(event);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Management thread error", e);
            }
        }
        log.info("Management thread stopped");
    }

    private void handle(DbEvent event) {
        boolean changed = statusRegistry.setState(
                event.poolId(), event.suggestedState(), event.reason());
        if (!changed) {
            return;
        }
        metrics.recordTransition(event.poolId(), event.suggestedState().name());
        StatusListener sl = listener.get();
        sl.onPoolStateChanged(event.poolId(), event.suggestedState().name(), event.reason());

        PoolStatusSnapshot snap = statusRegistry.current();
        sl.onSnapshot(snap);

        if (event.suggestedState() == PoolState.DOWN) {
            StatusListener.AlarmSeverity sev = snap.upCount() == 0
                    ? StatusListener.AlarmSeverity.CRITICAL
                    : StatusListener.AlarmSeverity.MAJOR;
            sl.onAlarm(sev, event.poolId(),
                    "Postgres pool DOWN: " + event.poolId() + " (" + event.reason() + ")");
        } else {
            sl.onAlarm(StatusListener.AlarmSeverity.CLEAR, event.poolId(),
                    "Postgres pool UP: " + event.poolId());
        }
        log.info("Pool {} -> {} [{}] upCount={}",
                event.poolId(), event.suggestedState(), event.source(), snap.upCount());
    }
}
