package com.npdb.cnf.connector;

import com.npdb.cnf.config.DbEndpointConfig;
import com.npdb.cnf.config.NpdbConnectorConfig;
import com.npdb.cnf.event.DbEvent;
import com.npdb.cnf.event.DbEventSource;
import com.npdb.cnf.event.ManagementThread;
import com.npdb.cnf.event.MonitoringThread;
import com.npdb.cnf.metrics.ConnectorMetrics;
import com.npdb.cnf.pool.HikariPoolFactory;
import com.npdb.cnf.pool.PoolRegistry;
import com.npdb.cnf.spi.StatusListener;
import com.npdb.cnf.status.PoolStatusRegistry;
import com.npdb.cnf.status.PoolStatusSnapshot;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

/**
 * Common DB connector used by DB Handler and Service OAM.
 * <ul>
 *   <li>One HikariCP pool per Multus static IP (max 3)</li>
 *   <li>Round-robin across UP pools for reads</li>
 *   <li>Management thread owns pool status; workers only enqueue events</li>
 *   <li>{@code initializationFailTimeout=0} — process starts even if all DBs are down</li>
 * </ul>
 */
public final class NpdbConnector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NpdbConnector.class);

    private final NpdbConnectorConfig config;
    private final PoolRegistry poolRegistry = new PoolRegistry();
    private final PoolStatusRegistry statusRegistry = new PoolStatusRegistry();
    private final HealthyPoolSelector selector;
    private final ConnectorMetrics metrics = new ConnectorMetrics();
    private final BlockingQueue<DbEvent> eventQueue = new ArrayBlockingQueue<>(1024);
    private final TimeoutProfileApplier timeoutApplier;
    private final ManagementThread managementRunnable;
    private final MonitoringThread monitoringRunnable;
    private Thread managementThread;
    private Thread monitoringThread;
    private volatile boolean started;

    public NpdbConnector(NpdbConnectorConfig config) {
        this.config = Objects.requireNonNull(config);
        this.selector = new HealthyPoolSelector(statusRegistry);
        this.timeoutApplier = new TimeoutProfileApplier(
                config.bulkNetworkTimeoutMs(),
                config.poolSettings().statementQueryTimeoutSec());
        this.managementRunnable = new ManagementThread(eventQueue, statusRegistry, metrics);
        this.monitoringRunnable = new MonitoringThread(
                poolRegistry, statusRegistry, eventQueue,
                config.healthSql(), config.monitoringIntervalMs());
    }

    /**
     * Create pools and start management + monitoring threads.
     * Safe when Postgres is unreachable (Hikari initializationFailTimeout=0).
     */
    public synchronized void start() {
        if (started) {
            return;
        }
        String appName = config.processType() == NpdbConnectorConfig.ProcessType.DB_HANDLER
                ? "npdb-handler" : "npdb-oam";
        statusRegistry.initialize(config.endpoints());
        for (DbEndpointConfig ep : config.endpoints()) {
            HikariDataSource ds = HikariPoolFactory.create(ep, config.poolSettings(), appName);
            poolRegistry.register(ep, ds);
            log.info("Created Hikari pool {} -> {}:{} ({})", ep.id(), ep.host(), ep.port(), ep.role());
        }
        managementThread = new Thread(managementRunnable, "npdb-management");
        managementThread.setDaemon(false);
        monitoringThread = new Thread(monitoringRunnable, "npdb-monitoring");
        monitoringThread.setDaemon(true);
        managementThread.start();
        monitoringThread.start();
        started = true;
        log.info("NpdbConnector started for {} with {} pools", config.processType(), config.endpoints().size());
    }

    public void setStatusListener(StatusListener listener) {
        managementRunnable.setStatusListener(listener);
    }

    public PoolStatusSnapshot statusSnapshot() {
        return statusRegistry.current();
    }

    public ConnectorMetrics metrics() {
        return metrics;
    }

    public PoolRegistry pools() {
        return poolRegistry;
    }

    /**
     * NP lookup — <b>no application retry</b> (final HLD). Failures enqueue DOWN events only.
     */
    public NpLookupResult lookup(String subscriberNumber) {
        ensureStarted();
        String poolId = selector.nextUpPoolId();
        if (poolId == null) {
            metrics.recordFailure(null);
            throw new NpdbException(NpdbException.Code.ALL_POOLS_UNAVAILABLE,
                    "No UP Postgres pools available for NP lookup");
        }
        long t0 = System.nanoTime();
        try {
            NpLookupResult result = executeLookup(poolId, subscriberNumber);
            metrics.recordSuccess(poolId, System.nanoTime() - t0);
            return result;
        } catch (NpdbException ex) {
            metrics.recordFailure(poolId);
            throw ex;
        } catch (SQLException ex) {
            metrics.recordFailure(poolId);
            publishDown(poolId, DbEventSource.WORKER, ex);
            throw new NpdbException(classify(ex), "NP lookup failed on pool " + poolId, poolId, ex);
        }
    }

    /**
     * Service OAM single-row DML against PRIMARY with up to {@code maxOamRetries} infra retries
     * across alternate UP pools when appropriate. Writes prefer primary.
     */
    public int executeOamUpdate(String sql, SqlBinder binder) {
        return executeOamUpdate(sql, binder, false);
    }

    /**
     * @param allowReplicaFallback when true, retries may use other UP pools (rare for writes)
     */
    public int executeOamUpdate(String sql, SqlBinder binder, boolean allowReplicaFallback) {
        ensureStarted();
        int attempts = 0;
        int maxAttempts = 1 + Math.max(0, config.maxOamRetries());
        String lastPool = null;
        SQLException last = null;
        while (attempts < maxAttempts) {
            attempts++;
            String poolId = selectOamWritePool(lastPool, allowReplicaFallback);
            if (poolId == null) {
                break;
            }
            lastPool = poolId;
            try {
                return executeUpdate(poolId, sql, binder, TimeoutProfileApplier.Kind.SHORT);
            } catch (SQLException ex) {
                last = ex;
                publishDown(poolId, DbEventSource.WORKER, ex);
                if (!isInfraFailure(ex) || attempts >= maxAttempts) {
                    throw new NpdbException(classify(ex), "OAM update failed on " + poolId, poolId, ex);
                }
            }
        }
        throw new NpdbException(NpdbException.Code.ALL_POOLS_UNAVAILABLE,
                "OAM update failed after retries", lastPool, last);
    }

    /**
     * Bulk / long operation: borrows PRIMARY connection, raises network timeout, runs callback, resets.
     */
    public <T> T executeBulkOnPrimary(Function<Connection, T> work) {
        ensureStarted();
        var primary = poolRegistry.findPrimary()
                .orElseThrow(() -> new NpdbException(NpdbException.Code.CONFIG, "No PRIMARY pool configured"));
        if (!statusRegistry.current().find(primary.id()).map(s -> s.isUp()).orElse(false)) {
            // Still try — monitoring may lag; infra failure will surface
            log.warn("PRIMARY {} not marked UP; attempting bulk anyway", primary.id());
        }
        try (Connection c = primary.dataSource().getConnection()) {
            timeoutApplier.apply(c, TimeoutProfileApplier.Kind.BULK);
            try {
                return work.apply(c);
            } finally {
                timeoutApplier.reset(c);
            }
        } catch (SQLException ex) {
            publishDown(primary.id(), DbEventSource.WORKER, ex);
            throw new NpdbException(classify(ex), "Bulk operation failed on primary", primary.id(), ex);
        }
    }

    public int executeBulkUpdate(String sql, SqlBinder binder) {
        return executeBulkOnPrimary(c -> {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                timeoutApplier.apply(ps, TimeoutProfileApplier.Kind.BULK);
                binder.bind(ps);
                return ps.executeUpdate();
            } catch (SQLException e) {
                throw new NpdbException(classify(e), "Bulk update failed", null, e);
            }
        });
    }

    private String selectOamWritePool(String excludeFailed, boolean allowReplicaFallback) {
        var primary = statusRegistry.primaryIfUp();
        if (primary.poolId() != null && primary.up()
                && (excludeFailed == null || !excludeFailed.equals(primary.poolId()))) {
            return primary.poolId();
        }
        if (!allowReplicaFallback) {
            // Final HLD: provisioning on primary — if primary down, still return primary id for attempt/fail
            return poolRegistry.findPrimary().map(PoolRegistry.ManagedPool::id).orElse(null);
        }
        if (excludeFailed == null) {
            return selector.nextUpPoolId();
        }
        return selector.nextUpPoolIdExcluding(excludeFailed);
    }

    private NpLookupResult executeLookup(String poolId, String subscriberNumber) throws SQLException {
        PoolRegistry.ManagedPool pool = poolRegistry.require(poolId);
        try (Connection c = pool.dataSource().getConnection();
             PreparedStatement ps = c.prepareStatement(config.npLookupSql())) {
            timeoutApplier.apply(c, TimeoutProfileApplier.Kind.SHORT);
            timeoutApplier.apply(ps, TimeoutProfileApplier.Kind.SHORT);
            ps.setString(1, subscriberNumber);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return NpLookupResult.notFound(subscriberNumber, poolId);
                }
                String routing = rs.getString(2);
                return NpLookupResult.found(subscriberNumber, routing, poolId);
            } finally {
                timeoutApplier.reset(c);
            }
        }
    }

    private int executeUpdate(String poolId, String sql, SqlBinder binder,
                              TimeoutProfileApplier.Kind kind) throws SQLException {
        PoolRegistry.ManagedPool pool = poolRegistry.require(poolId);
        try (Connection c = pool.dataSource().getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            timeoutApplier.apply(c, kind);
            timeoutApplier.apply(ps, kind);
            binder.bind(ps);
            try {
                int n = ps.executeUpdate();
                metrics.recordSuccess(poolId, 0L);
                return n;
            } finally {
                timeoutApplier.reset(c);
            }
        }
    }

    private void publishDown(String poolId, DbEventSource source, SQLException ex) {
        String reason = ex.getClass().getSimpleName() + ": " + ex.getMessage();
        if (!eventQueue.offer(DbEvent.down(poolId, source, reason))) {
            log.warn("Event queue full while reporting DOWN for {}", poolId);
        }
    }

    private static boolean isInfraFailure(SQLException ex) {
        if (ex instanceof SQLTimeoutException) {
            return true;
        }
        String state = ex.getSQLState();
        if (state != null && (state.startsWith("08") || state.equals("57P01") || state.equals("57P02"))) {
            return true;
        }
        String msg = String.valueOf(ex.getMessage()).toLowerCase();
        return msg.contains("connection") || msg.contains("broken pipe")
                || msg.contains("reset") || msg.contains("closed") || msg.contains("timeout");
    }

    private static NpdbException.Code classify(SQLException ex) {
        String msg = String.valueOf(ex.getMessage()).toLowerCase();
        if (msg.contains("connection is not available") || msg.contains("timed out")) {
            return NpdbException.Code.ACQUIRE_TIMEOUT;
        }
        return NpdbException.Code.QUERY_FAILED;
    }

    private void ensureStarted() {
        if (!started) {
            throw new NpdbException(NpdbException.Code.CONFIG, "NpdbConnector not started");
        }
    }

    @Override
    public synchronized void close() {
        if (!started) {
            return;
        }
        monitoringRunnable.requestStop();
        managementRunnable.requestStop();
        if (monitoringThread != null) {
            monitoringThread.interrupt();
        }
        if (managementThread != null) {
            managementThread.interrupt();
        }
        poolRegistry.closeAll();
        started = false;
        log.info("NpdbConnector closed");
    }

    @FunctionalInterface
    public interface SqlBinder {
        void bind(PreparedStatement ps) throws SQLException;
    }
}
