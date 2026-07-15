package com.npdb.cnf.oam;

import com.npdb.cnf.config.NpdbConnectorConfig;
import com.npdb.cnf.connector.NpdbConnector;
import com.npdb.cnf.spi.StatusListener;
import com.npdb.cnf.status.PoolStatusSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Service OAM process facade: single-threaded main path + one background thread
 * for bulk / audit / backup / restore long operations (final HLD).
 * <p>
 * All provisioning DML uses PRIMARY connections. Bulk raises
 * {@code Connection.setNetworkTimeout} for the operation duration then resets.
 */
public final class ServiceOamDbService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ServiceOamDbService.class);

    private final NpdbConnector connector;
    private final ExecutorService background;

    public ServiceOamDbService(NpdbConnectorConfig config) {
        if (config.processType() != NpdbConnectorConfig.ProcessType.SERVICE_OAM) {
            throw new IllegalArgumentException("config must be SERVICE_OAM");
        }
        this.connector = new NpdbConnector(config);
        this.background = Executors.newSingleThreadExecutor(new ThreadFactory() {
            private final AtomicInteger n = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "npdb-oam-bulk-" + n.incrementAndGet());
                t.setDaemon(false);
                return t;
            }
        });
    }

    public void start() {
        connector.start();
        log.info("Service OAM DB service started");
    }

    public void setStatusListener(StatusListener listener) {
        connector.setStatusListener(listener);
    }

    /**
     * Single-row provisioning on PRIMARY (main OAM thread). Retries up to maxOamRetries on infra failure.
     */
    public int provisionSingle(String sql, NpdbConnector.SqlBinder binder) {
        return connector.executeOamUpdate(sql, binder, false);
    }

    /**
     * Submit bulk load to the background thread. Caller supplies ordered batches of bind actions.
     * Each batch borrows PRIMARY, applies BULK timeout profile, executes, resets.
     */
    public Future<BulkLoadResult> submitBulkProvision(String sql, List<NpdbConnector.SqlBinder> batches) {
        Objects.requireNonNull(batches);
        return background.submit(() -> {
            int total = 0;
            int batchNo = 0;
            for (NpdbConnector.SqlBinder binder : batches) {
                batchNo++;
                int n = connector.executeBulkUpdate(sql, binder);
                total += n;
                log.info("Bulk batch {} updated {} row(s), cumulative={}", batchNo, n, total);
            }
            return new BulkLoadResult(batches.size(), total);
        });
    }

    /**
     * Generic long-running job (audit / backup / restore) on background thread with BULK timeout profile.
     */
    public Future<Void> submitLongJob(String name, Consumer<java.sql.Connection> job) {
        return background.submit(() -> {
            log.info("Starting OAM long job: {}", name);
            connector.executeBulkOnPrimary(c -> {
                job.accept(c);
                return null;
            });
            log.info("Finished OAM long job: {}", name);
            return null;
        });
    }

    /**
     * Example audit helper: runs a read-only SQL statement under BULK timeouts on PRIMARY
     * (switch to replica in a future enhancement if required).
     */
    public Future<Integer> submitAuditQuery(String sql) {
        return background.submit(() -> connector.executeBulkOnPrimary(c -> {
            try (Statement st = c.createStatement()) {
                // Statement timeout already applied via connection network timeout profile
                try (var rs = st.executeQuery(sql)) {
                    int rows = 0;
                    while (rs.next()) {
                        rows++;
                    }
                    return rows;
                }
            } catch (Exception e) {
                throw new RuntimeException("Audit query failed", e);
            }
        }));
    }

    public PoolStatusSnapshot poolStatus() {
        return connector.statusSnapshot();
    }

    public NpdbConnector connector() {
        return connector;
    }

    @Override
    public void close() {
        background.shutdownNow();
        connector.close();
        log.info("Service OAM DB service stopped");
    }

    public record BulkLoadResult(int batches, int rowsAffected) {
    }
}
