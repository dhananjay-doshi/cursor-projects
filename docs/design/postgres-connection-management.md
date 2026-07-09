# Postgres Connection Management Module — Design

**Scope:** Connection management and custom pooling for Number Portability (NP) queries against Postgres DB PODs.  
**Out of scope:** Network / Multus / CNI design (static Multus IPs are assumed available and mandatory).  
**Target:** Telco-grade CNF — HA, low latency, **≥ 5000 queries/sec**, **p99 query latency < 20 ms**.

---

## 1. Requirements Summary

| Area | Requirement |
|------|-------------|
| Runtime | Java; HikariCP for Postgres access |
| Topology | App client POD ↔ 3 Postgres PODs (1 Primary + 2 Replica), Multus **static IPs** |
| Workload | Read-heavy NP lookups (subscriber → routing number); prepared statements mandatory |
| Threads | 16 worker threads + 1 management thread (+ optional monitor) |
| Failure policy | Failed query is **not** retried by this module; connection loss must be marked and pool repaired |
| Status | Connection up/down events → management thread → alarms + broadcast to core modules |
| Metrics | Queries/connection, failed vs available connections, per-POD health & load |

### Non-negotiable constraints

1. Connections must target DB PODs via **fixed Multus static IPs**.
2. Prepared statements must be used on live connections for query efficiency.
3. Design limited to connection management / pooling / LB — not network redesign.

---

## 2. Critique of the Proposed Design

The proposed design correctly retains the 16-worker + management-thread model and the need for Multus static IPs, monitoring, and metrics. Several points need correction for production readiness with HikariCP.

### 2.1 One HikariCP pool cannot span three JDBC URLs

HikariCP binds a pool to **one** `jdbcUrl` / `DataSource`. A single pool with “connections to three DB instances” is not supported.

**Required change:** Use **three independent HikariCP pools** — one per Multus static IP (Primary, Replica-1, Replica-2).

### 2.2 Do not maintain a parallel “list of 16 connections” outside HikariCP

Owning raw `Connection` objects in an application list while also using HikariCP causes:

- Double lifecycle ownership (leaks, stale handles, pool exhaustion)
- Broken validation / eviction (`maxLifetime`, keepalive, leak detection)
- Race conditions when workers and management both mutate the list

**Required change:** Workers **borrow / return** connections via HikariCP (`getConnection()` / `close()`). The module may keep a **logical slot map** (worker → preferred POD / pool) for affinity and LB accounting, but not long-lived borrowed connections held across idle periods unless using a deliberate sticky-borrow pattern with strict timeouts (not recommended for CNF).

### 2.3 Prepared statements and pooling

Prepared statements are tied to a physical connection. After return to the pool, another worker may get a different physical connection.

**Recommended approach (production):**

1. Enable **server-side prepared statement caching** via the Postgres JDBC driver:
   - `prepareThreshold=1` (or small N)
   - `preparedStatementCacheQueries` sized for the NP statement set
2. Optionally use HikariCP `connectionInitSql` only for session setup (e.g. `SET` options), not for app prepared statements.
3. Each query path: borrow connection → `prepareStatement` / reuse driver-cached PS → execute → close statement → return connection.

This preserves prepared-statement efficiency without pinning 16 physical connections forever.

**Alternative (closer to current VNF behavior):** Sticky connection per worker (borrow once, hold until failure). This reduces pool churn and matches today’s mapping, but:

- Reduces effective pool sharing
- Complicates failover (worker must re-bind on loss)
- Must still use **three pools** and a supervisor to reassign workers when a POD dies

For telco NP with fixed 16 workers and stable load, **sticky-per-worker across three pools** is acceptable and maps well to the existing VNF design — provided ownership remains “worker holds one connection from one of three pools” and failed connections are closed back to HikariCP so the pool can replace them.

### 2.4 “6 minimum idle / 16 maximum” on a single pool

With three PODs and equal distribution, size **per pool**, not globally:

| Setting | Suggested starting point | Rationale |
|---------|--------------------------|-----------|
| `maximumPoolSize` per POD pool | 6–8 | 16 workers ÷ 3 ≈ 5–6 active; headroom for monitor + reconnect |
| `minimumIdle` per POD pool | 4–6 | Keep warm connections; avoid cold connect under load |
| Global concurrent DB sessions | ≤ 3 × maxPoolSize | Cap against Postgres `max_connections` |

Exact numbers must fit Postgres `max_connections` across all app replicas (if multiple client PODs exist).

### 2.5 Primary vs Replica for NP reads

NP subscriber/routing lookups are typically **read-only**. Prefer:

- **Default LB:** round-robin / least-loaded across **healthy** PODs (Primary + Replicas), **or** replicas-only if primary must be reserved for writes / replication lag sensitivity is acceptable.
- **Policy knob:** `ReadPreference = ANY | REPLICAS_PREFERRED | PRIMARY_ONLY`

Document replication lag tolerance. If lag can return stale routing numbers, either:

- Use `PRIMARY_ONLY` for correctness-critical paths, or
- Monitor lag and mark lagging replicas **unready** (not only TCP-down).

### 2.6 No retry is fine; connection repair is mandatory

Agree: do not retry the failed query in this module. Still must:

1. Mark connection/POD unhealthy (or at least that slot failed)
2. Close bad connection so HikariCP replaces it
3. Rebalance worker affinity away from down PODs
4. Emit up/down events to management for alarms / broadcast

---

## 3. Recommended Architecture

### 3.1 Component diagram

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                     Application Client Process (POD)                      │
│                                                                         │
│  ┌──────────────┐   borrow/return    ┌───────────────────────────────┐ │
│  │ Worker 0..15 │ ─────────────────► │  ConnectionRouter / LB        │ │
│  │ (NP queries) │ ◄── ConnectionRef  │  - pick healthy pool           │ │
│  └──────┬───────┘   + metrics        │  - sticky or RR policy         │ │
│         │ up/down                    └──────────────┬────────────────┘ │
│         │ events                                    │                  │
│         ▼                                           ▼                  │
│  ┌──────────────┐                    ┌──────────────────────────────┐  │
│  │ Management   │ ◄── health events  │ PoolManager                  │  │
│  │ Thread       │                    │  HikariCP Pool[Primary IP]   │  │
│  │ - alarms     │                    │  HikariCP Pool[Replica1 IP]  │  │
│  │ - broadcast  │                    │  HikariCP Pool[Replica2 IP]  │  │
│  │ - POD state  │                    └──────────────────────────────┘  │
│  └──────────────┘                                   ▲                  │
│         ▲                                           │                  │
│         │                      ┌────────────────────┴───────────────┐  │
│         └──────────────────────│ HealthMonitor Thread               │  │
│                                │  - 1 probe connection per POD pool │  │
│                                │  - SELECT 1 / lightweight NP probe │  │
│                                └────────────────────────────────────┘  │
│                                                                         │
│  MetricsExporter: qps/conn, fail counts, pool active/idle, POD state    │
└─────────────────────────────────────────────────────────────────────────┘
                 │ Multus static IP          │                    │
                 ▼                           ▼                    ▼
          Postgres Primary            Replica-1              Replica-2
```

### 3.2 Module breakdown

| Module | Responsibility |
|--------|----------------|
| **PoolManager** | Create/configure 3 HikariCP pools from static IP config; lifecycle start/stop; expose pool by `DbInstanceId` |
| **ConnectionRouter** | Select target pool (LB + health); sticky affinity for workers; record metrics |
| **WorkerSession** (optional) | Per-worker sticky `Connection` + prepared statement handle; invalidate on SQLException/connection loss |
| **HealthMonitor** | Periodic probe **one connection per POD pool**; publish UP/DOWN/DEGRADED |
| **ManagementBridge** | Serialize status events to existing management thread; alarm + broadcast |
| **MetricsRegistry** | Per-connection / per-POD / per-pool counters and gauges |

---

## 4. Connection & Threading Model (Recommended)

### 4.1 Sticky-per-worker (closest to current VNF, recommended for this app)

Retain the mental model: each worker owns one DB connection for query processing.

```text
Startup (main thread):
  1. Load config: 3 × {instanceId, staticIp, port, credentials, role}
  2. PoolManager.createPools()  // 3 HikariCP DataSources
  3. ConnectionRouter.assignInitialAffinity(16 workers)  // ~equal across healthy PODs
  4. Each worker: borrow connection from assigned pool, prepare NP statement(s)
  5. Start HealthMonitor + hand ManagementBridge to management thread

Steady state:
  Worker_i:
    on query:
      execute prepared statement on sticky connection
      metrics.increment(queries, connectionId, podId)
    on SQLException / connection invalid:
      emit CONNECTION_DOWN(connectionId, podId, cause)
      close connection (return to pool / discard)
      ask ConnectionRouter for next healthy pool (LB)
      borrow + prepare again
      do NOT retry the failed business query

HealthMonitor:
  every T ms (e.g. 200–500 ms for telco):
    for each POD pool:
      borrow (or use dedicated monitor connection)
      run probe; measure RTT
      if fail → POD_DOWN; if RTT > threshold → POD_DEGRADED
      emit events only on state transitions (edge-triggered)

Management thread:
  maintain DbInstanceState[3]
  on transition → alarm + broadcast to core modules
  optionally ask ConnectionRouter to drain affinity from DOWN pods
```

**Fair distribution:** At assignment time and on rebind, use **least-assigned healthy POD** (or weighted RR). Do not scan a shared mutable “16 connection list” under lock on every query — that becomes a latency hotspot at 5k qps.

### 4.2 Why not scan a shared connection list per query?

At 5000 qps with 16 workers, per-query list scanning + locking adds jitter and contention. Prefer:

- **O(1)** sticky connection on the worker (ThreadLocal / worker struct)
- Rebalance only on failure or health transition

### 4.3 Pool sizing formula

```text
workers = 16
pods = 3
monitor_conns = 1 per pod   // can share pool or use separate max
per_pool_max = ceil(workers / pods) + monitor_headroom + reconnect_headroom
             ≈ 6 + 1 + 1 = 8

per_pool_min_idle ≈ ceil(workers / pods) = 6
```

HikariCP tips for low latency:

- `connectionTimeout`: small enough to fail fast (e.g. 250–1000 ms) — failed query is not retried anyway
- `validationTimeout`: short (e.g. 200–500 ms)
- `keepaliveTime`: enabled (HikariCP 5.x) to avoid idle disconnects through firewalls / CNI
- `maxLifetime`: longer than peak idle gaps, shorter than server/firewall idle drop
- `leakDetectionThreshold`: enabled in lower environments; careful in prod (overhead)

JDBC URL must use Multus static IP host, e.g.:

```text
jdbc:postgresql://<MULTUS_IP>:<PORT>/<DB>?prepareThreshold=1&tcpKeepAlive=true
```

---

## 5. Load Balancing Strategy

### 5.1 Goals

1. Equal query distribution across **healthy** DB instances under steady state  
2. Immediate drain from DOWN instances  
3. Minimal cross-worker coordination  

### 5.2 Algorithm

**Initial / rebalance assignment (management or router):**

```text
healthy = filter(pods, state == UP)
sort healthy by (activeWorkerCount asc, queryCount asc)
assign unbound workers round-robin / least-loaded
```

**Per-query:** no LB decision — use sticky connection (lowest latency path).

**On POD_DOWN:**

```text
for each worker affinity == downPod:
  invalidate sticky connection
  reassign to least-loaded healthy pod
  (next query uses new connection; failed query already abandoned)
```

**On POD_UP:**

```text
gradually rebalance: move workers from most-loaded → recovered pod
  (optional hysteresis: wait N successful probes before UP)
```

### 5.3 Avoiding message / status loss

“Avoid message loss” in the brief is interpreted as **status broadcast reliability**, not query retry:

- Health and connection events go through a **single-producer-friendly queue** to the management thread (e.g. `ArrayBlockingQueue` / Disruptor-style ring if already used in the app)
- Management thread is the **only** writer of `DbInstanceState`
- Broadcasts are edge-triggered with last-known state recoverable on subscriber reconnect
- Workers never block indefinitely on management; event offer with metric on drop (should be near-zero)

---

## 6. Health Monitoring & Alarms

### 6.1 Probe design

| Item | Recommendation |
|------|----------------|
| Probe SQL | `SELECT 1` (or `SELECT 1 FROM np_table LIMIT 1` if deeper check needed) |
| Frequency | 200–500 ms per POD (tune vs load) |
| Failure threshold | N consecutive failures (e.g. 2–3) before DOWN |
| Recovery threshold | M consecutive successes (e.g. 3–5) before UP (hysteresis) |
| Degraded | Probe succeeds but RTT > SLA budget share (e.g. > 5–8 ms) |

Use **one probe path per POD pool**, not one probe for all PODs on a single connection.

### 6.2 Event types

```text
CONNECTION_DOWN   { workerId, podId, connectionId, errorCode, ts }
CONNECTION_UP     { workerId, podId, connectionId, ts }
POD_DOWN          { podId, reason, ts }
POD_DEGRADED      { podId, rttMs, ts }
POD_UP            { podId, ts }
POOL_EXHAUSTED    { podId, waitMs, ts }   // useful for capacity alarms
```

Worker threads and HealthMonitor both may emit; Management thread deduplicates POD-level state.

---

## 7. Failure Handling Matrix

| Failure | Worker behavior | Pool / Router | Management |
|---------|-----------------|---------------|------------|
| Query SQL error (data) | Return error to caller; no retry | No pool change | Optional metric only |
| Connection reset / EOF | Fail query; close conn; rebind | HikariCP replaces physical conn | CONNECTION_DOWN → maybe POD check |
| POD unreachable | Fail query; mark affinity invalid | Router stops assigning POD | POD_DOWN + alarm + broadcast |
| Pool timeout on borrow | Fail query | Metric POOL_EXHAUSTED | Alarm if sustained |
| Partial brownout (high RTT) | Continue if sticky still works | Prefer other PODs on rebind | POD_DEGRADED |

**HikariCP re-establishment:** Closing the failed `Connection` returns it to the pool as dead; HikariCP discards and creates a new one in the background / on next borrow. Do **not** cache closed JDBC objects. Recreate `PreparedStatement` after every new borrow.

**Timeouts:** Prefer failing the individual query quickly over long JDBC waits that blow the 20 ms budget for other work on that worker.

---

## 8. Metrics (Minimum Set)

### Per connection (logical worker sticky id)

- `np_db_queries_total{pod,worker,connection}`
- `np_db_query_failures_total{pod,worker,reason}`
- `np_db_query_latency_ms` (histogram; track p50/p99 vs 20 ms SLA)

### Per POD / pool

- `np_db_pool_active{pod}`
- `np_db_pool_idle{pod}`
- `np_db_pool_waiting{pod}`
- `np_db_connections_available{pod}`
- `np_db_connections_failed_total{pod}`
- `np_db_pod_state{pod}` (0=DOWN, 1=DEGRADED, 2=UP)
- `np_db_probe_rtt_ms{pod}`
- `np_db_queries_total{pod}` (for LB fairness dashboards)

Export via the platform’s existing metrics path (JMX / Prometheus / internal counters).

---

## 9. Configuration Sketch

```yaml
npDb:
  readPreference: ANY          # ANY | REPLICAS_PREFERRED | PRIMARY_ONLY
  workerCount: 16
  stickyConnections: true
  health:
    intervalMs: 250
    failThreshold: 3
    recoverThreshold: 5
    degradedRttMs: 8
  pools:
    - id: primary
      role: PRIMARY
      jdbcUrl: jdbc:postgresql://10.10.1.1:5432/npdb?prepareThreshold=1&tcpKeepAlive=true
      maxPoolSize: 8
      minIdle: 6
      connectionTimeoutMs: 500
      validationTimeoutMs: 300
      keepaliveTimeMs: 30000
      maxLifetimeMs: 1800000
    - id: replica-1
      role: REPLICA
      jdbcUrl: jdbc:postgresql://10.10.1.2:5432/npdb?prepareThreshold=1&tcpKeepAlive=true
      maxPoolSize: 8
      minIdle: 6
      # ...same timeouts...
    - id: replica-2
      role: REPLICA
      jdbcUrl: jdbc:postgresql://10.10.1.3:5432/npdb?prepareThreshold=1&tcpKeepAlive=true
      maxPoolSize: 8
      minIdle: 6
```

Credentials via secrets mount / vault — not hardcoded.

---

## 10. Sequence: Query Path (Happy Path)

```text
Caller → Worker_i.queue
Worker_i:
  ps = sticky.preparedStatement
  rs = ps.executeQuery(subscriber)
  map row → routing number
  metrics.ok(pod, worker, latency)
  return result
```

Target budget inside module (illustrative):

| Step | Budget |
|------|--------|
| Execute + fetch | < 10–15 ms |
| Module overhead (no lock scan) | < 1 ms |
| Remaining for app framing | rest of < 20 ms SLA |

---

## 11. Sequence: Connection Loss

```text
Worker_i execute → SQLException (connection broken)
Worker_i:
  metrics.fail(...)
  emit CONNECTION_DOWN
  sticky.closeQuietly()
  pod = router.pickHealthy(exclude=optional)
  sticky = poolManager.get(pod).getConnection()
  sticky.prepare(NP_SQL)
  emit CONNECTION_UP
  return query failure to caller   // no retry
Management:
  update maps; if pod probes also failing → POD_DOWN → alarm + broadcast
HealthMonitor:
  continues probing; on recover → POD_UP → router may rebalance
```

---

## 12. Comparison: Proposed vs Recommended

| Topic | Proposed | Recommended |
|-------|----------|-------------|
| HikariCP pools | One pool, 3 DBs | **Three pools**, one per Multus IP |
| Connection list of 16 | App-owned shared list | **Sticky per worker** + HikariCP ownership |
| Per-query LB scan | Scan list each time | **Sticky**; rebalance on health events |
| Prepared statements | Precreate on list | Recreate on borrow/rebind; driver cache assists |
| minIdle/max | 6 / 16 global | **~6 / 8 per pool** (tune to `max_connections`) |
| Monitor | Dummy query one conn per POD | Keep; add hysteresis + RTT degraded state |
| Retry | None | None (unchanged) |
| Status | Mgmt thread | Mgmt thread + edge-triggered queue |

---

## 13. Performance Considerations (5000 qps / < 20 ms)

1. **No shared lock on the query path** — sticky connection in worker-local state.  
2. **Prepared statements** + Postgres JDBC server-prepare cache.  
3. **Fail-fast timeouts** so a hung POD does not stall workers beyond budget.  
4. **Warm pools** (`minimumIdle`) to avoid connect handshake during traffic.  
5. **tcpKeepAlive** + HikariCP keepalive to reduce silent dead connections through the CNF network path.  
6. **Equal worker affinity** across PODs ≈ equal query distribution under uniform worker load.  
7. Size Postgres and OS resources for `3 × maxPoolSize × (number of client replicas)`.  
8. Pin critical threads / use existing app thread model; avoid creating extra pools of threads beyond monitor + management.

Rough capacity check: 5000 qps / 16 workers ≈ **312 qps/worker** ≈ **3.2 ms average** spacing between queries per worker — compatible with < 20 ms latency if DB RTT stays low on Multus.

---

## 14. Implementation Outline (Java packages)

```text
com.example.np.db
  ├── PoolManager
  ├── DbInstanceConfig / DbInstanceId / DbRole
  ├── ConnectionRouter
  ├── WorkerDbSession          // sticky Connection + PreparedStatement
  ├── HealthMonitor
  ├── DbStatusEvent / DbInstanceState
  ├── ManagementStatusBridge
  └── metrics/DbMetrics
```

Suggested build order:

1. PoolManager + config (3 pools over static IPs)  
2. WorkerDbSession sticky borrow/prepare/execute/invalidate  
3. ConnectionRouter LB + health gating  
4. HealthMonitor + ManagementStatusBridge  
5. Metrics + soak test at ≥ 5000 qps  
6. Chaos: kill replica POD, primary failover, network partition — verify alarms, no stuck workers, fairness restored  

---

## 15. Open Decisions (product / ops)

1. **Read preference:** ANY (primary+replicas) vs REPLICAS_PREFERRED vs PRIMARY_ONLY given replication lag risk for NP.  
2. **Multiple app client replicas:** confirm total connection budget vs Postgres `max_connections`.  
3. **Monitor interval vs CPU:** 250 ms × 3 probes may be fine; validate in lab.  
4. **Whether DEGRADED pods still accept sticky traffic** or are drain-only.  
5. **Primary failover:** if primary Multus IP moves / VIP changes — static IP assumption says IP is stable; confirm ops contract when primary role moves between PODs.

---

## 16. Design Verdict

Keep the **16 workers + management thread + Multus static IPs + no query retry + prepared statements + per-POD monitoring** from the proposed design.

Change the pooling model to **three HikariCP pools**, **sticky-per-worker sessions** (instead of a shared scanned connection list), **health-gated least-loaded assignment**, and **fail-fast reconnect** so HikariCP can re-establish connections without double ownership.

This yields fair load across the three DB PODs, predictable low-latency query paths, and clean DB status maintenance for alarms and core-module broadcast under CNF deployment.
