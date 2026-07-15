package com.npdb.cnf.connector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Applies / resets per-operation timeout profiles on a borrowed connection
 * (Service OAM bulk uses extended network timeout per final HLD).
 */
public final class TimeoutProfileApplier {

    public enum Kind {
        SHORT,   // single NP / single OAM DML — URL socketTimeout applies
        BULK     // long bulk / audit — raise network timeout then revert
    }

    private final Executor timeoutExecutor;
    private final long bulkNetworkTimeoutMs;
    private final int shortQueryTimeoutSec;

    public TimeoutProfileApplier(long bulkNetworkTimeoutMs, int shortQueryTimeoutSec) {
        this.bulkNetworkTimeoutMs = bulkNetworkTimeoutMs;
        this.shortQueryTimeoutSec = shortQueryTimeoutSec;
        this.timeoutExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            private final AtomicInteger n = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "npdb-timeout-" + n.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void apply(Connection connection, Kind kind) throws SQLException {
        if (kind == Kind.BULK) {
            connection.setNetworkTimeout(timeoutExecutor, (int) Math.min(Integer.MAX_VALUE, bulkNetworkTimeoutMs));
        } else {
            // Leave driver socketTimeout; clear any prior bulk network timeout
            connection.setNetworkTimeout(timeoutExecutor, 0);
        }
    }

    public void apply(Statement statement, Kind kind) throws SQLException {
        if (kind == Kind.BULK) {
            // seconds; align roughly with bulk network budget (e.g. 5 min -> 300s)
            int sec = (int) Math.max(1, bulkNetworkTimeoutMs / 1000L);
            statement.setQueryTimeout(sec);
        } else if (shortQueryTimeoutSec > 0) {
            statement.setQueryTimeout(shortQueryTimeoutSec);
        } else {
            statement.setQueryTimeout(0);
        }
    }

    public void reset(Connection connection) throws SQLException {
        connection.setNetworkTimeout(timeoutExecutor, 0);
    }
}
