package com.npdb.cnf.handler;

import com.npdb.cnf.config.NpdbConnectorConfig;
import com.npdb.cnf.connector.NpLookupResult;
import com.npdb.cnf.connector.NpdbConnector;
import com.npdb.cnf.spi.StatusListener;
import com.npdb.cnf.status.PoolStatusSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DB Handler process facade: 16 worker threads via {@link ThreadPoolExecutor}
 * + {@link LinkedBlockingQueue}, backed by common {@link NpdbConnector}.
 * <p>
 * Management and monitoring threads live inside the connector.
 */
public final class DbHandlerService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DbHandlerService.class);

    private final NpdbConnector connector;
    private final ThreadPoolExecutor workers;

    public DbHandlerService(NpdbConnectorConfig config) {
        if (config.processType() != NpdbConnectorConfig.ProcessType.DB_HANDLER) {
            throw new IllegalArgumentException("config must be DB_HANDLER");
        }
        this.connector = new NpdbConnector(config);
        this.workers = new ThreadPoolExecutor(
                16, 16,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new WorkerThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void start() {
        connector.start();
        log.info("DB Handler service started (16 workers)");
    }

    public void setStatusListener(StatusListener listener) {
        connector.setStatusListener(listener);
    }

    /**
     * Submit NP lookup to the worker pool (async).
     */
    public Future<NpLookupResult> submitLookup(String subscriberNumber) {
        return workers.submit(() -> connector.lookup(subscriberNumber));
    }

    /**
     * Synchronous lookup on calling thread (tests / controlled callers).
     * Production traffic should use {@link #submitLookup(String)} so work runs on the 16 workers.
     */
    public NpLookupResult lookup(String subscriberNumber) {
        return connector.lookup(subscriberNumber);
    }

    public <T> Future<T> submit(Callable<T> task) {
        return workers.submit(task);
    }

    public PoolStatusSnapshot poolStatus() {
        return connector.statusSnapshot();
    }

    public NpdbConnector connector() {
        return connector;
    }

    @Override
    public void close() {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
        connector.close();
        log.info("DB Handler service stopped");
    }

    private static final class WorkerThreadFactory implements ThreadFactory {
        private final AtomicInteger n = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "npdb-worker-" + n.incrementAndGet());
            t.setDaemon(false);
            return t;
        }
    }
}
