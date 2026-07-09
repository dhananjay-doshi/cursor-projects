# Postgres Connection Management — CNF Design

**Scope:** Connection management, pooling, load balancing, health/status, and metrics for Number Portability (NP) queries against Postgres DB PODs.  
**Out of scope:** Multus / CNI / network design (static Multus IPs are assumed available and **mandatory**).  
**Target:** Telco-grade CNF — high availability, low latency, **≥ 5000 queries/sec**, **query latency < 200 ms**.

---

## 1. Requirements Summary

| Area | Requirement |
|------|-------------|
| Runtime | Java; connection pool framework (HikariCP) for Postgres |
| Topology | App client POD ↔ 3 Postgres PODs (1 Primary + 2 Replica), Multus **static IPs** |
| Workload | NP lookups (subscriber → routing number); **prepared statements required** |
| Threads | 16 worker threads + 1 management thread (+ monitoring thread) |
| Failure policy | Failed query is **not** retried by this module; connection loss must be marked and connection set repaired |
| Status | Connection up/down events → management thread → alarms + broadcast to core modules |
| Metrics | Queries per connection; connections failed vs available; per-POD health and load |

### Non-negotiable constraints

1. Connections must target DB PODs via **fixed Multus static IPs**.
2. Prepared statements must be used on live connections for query efficiency.
3. Design is limited to querying these databases with proper load balancing — not network redesign.

### Current VNF baseline (to preserve where useful)

- App client and Postgres co-located on the same node.
- 16 workers mapped to DB connections; prepared statements per connection.
- On connection termination: get a new connection from the pool, recreate prepared statement.
- One management thread broadcasts DB up/down status to other core modules.

---

## 2. Critique of the Proposed Design

The proposal correctly keeps 16 workers + management thread, Multus static IPs, monitoring, no query retry, and metrics. Several points need correction for production use with HikariCP under CNF.

### 2.1 One HikariCP pool cannot span three JDBC URLs

HikariCP binds a pool to **one** `jdbcUrl` / `DataSource`. A single pool with “connections to three DB instances” is not supported.

**Required change:** Use **three independent HikariCP pools** — one per Multus static IP (Primary, Replica-1, Replica-2).

### 2.2 Do not maintain a parallel “list of 16 connections” outside HikariCP

Owning raw `Connection` objects in an application-shared list while also using HikariCP causes:

- Double lifecycle ownership (leaks, stale handles, pool exhaustion)
- Broken validation / eviction (`maxLifetime`, keepalive, leak detection)
- Race conditions when workers and management both mutate the list
- Per-query list scanning under lock — a latency hotspot at 5k qps

**Required change:** Prefer **sticky-per-worker** sessions (worker holds one borrowed connection until failure), with HikariCP owning physical connection lifecycle. On failure: close → rebind from a healthy pool → recreate prepared statement. Do **not** scan a shared mutable list on every query.

### 2.3 Prepared statements and pooling

Prepared statements are tied to a physical connection. After return to the pool, another worker may get a different physical connection.

**Recommended for this app (matches VNF behavior):** Sticky connection per worker — prepare once after borrow; recreate only on rebind.

**Also enable** Postgres JDBC server-side prepare caching as a safety net:

- `prepareThreshold=1`
- `preparedStatementCacheQueries` sized for the NP statement set
- `tcpKeepAlive=true`

### 2.4 Pool sizing must be per POD, not global

Proposed “6 minimum idle / 16 maximum” on one pool does not map to three instances.

| Setting | Suggested starting point (per POD pool) | Rationale |
|---------|----------------------------------------|-----------|
| `maximumPoolSize` | 6–8 | ~16 workers ÷ 3 ≈ 5–6 active; headroom for monitor + reconnect |
| `minimumIdle` | 4–6 | Keep warm connections; avoid cold connect under load |
| Global concurrent sessions | ≤ 3 × maxPoolSize × (client POD count) | Must fit Postgres `max_connections` |

### 2.5 Primary vs Replica for NP reads

NP lookups are typically **read-only**. Prefer equal distribution across **healthy** PODs (Primary + Replicas), with a configurable read preference if replication lag can return stale routing numbers.

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
│  ┌──────────────┐   sticky execute       ┌───────────────────────────┐ │
│  │ Worker 0..15 │ ─────────────────────► │  ConnectionRouter / LB    │ │
│  │ (NP queries) │ ◄── WorkerDbSession    │  - pick healthy pool      │ │
│  └──────┬───────┘   + metrics            │  - sticky affinity        │ │
│         │ up/down                        └─────────────┬─────────────┘ │
│         │ events                                       │               │
│         ▼                                              ▼               │
│  ┌──────────────┐                     ┌────────────────────────────┐   │
│  │ Management   │ ◄── health events   │ PoolManager                │   │
│  │ Thread       │                     │  HikariCP[Primary Multus]  │   │
│  │ - alarms     │                     │  HikariCP[Replica1 Multus] │   │
│  │ - broadcast  │                     │  HikariCP[Replica2 Multus] │   │
│  │ - POD state  │                     └────────────────────────────┘   │
│  └──────────────┘                                   ▲                  │
│         ▲                                           │                  │
│         │                      ┌────────────────────┴───────────────┐  │
│         └──────────────────────│ HealthMonitor Thread               │  │
│                                │  - 1 probe path per POD pool       │  │
│                                │  - SELECT 1 / lightweight probe    │  │
│                                └────────────────────────────────────┘  │
│                                                                         │
│  Metrics: qps/conn, fail/available, pool active/idle, POD state         │
└─────────────────────────────────────────────────────────────────────────┘
                 │ Multus static IP          │                    │
                 ▼                           ▼                    ▼
          Postgres Primary            Replica-1              Replica-2
```

### 3.2 Module breakdown

| Module | Responsibility |
|--------|----------------|
| **PoolManager** | Create/configure 3 HikariCP pools from static IP config; lifecycle start/stop; expose pool by `DbInstanceId` |
| **ConnectionRouter** | Select target pool (LB + health); sticky affinity for workers; record assignment metrics |
| **WorkerDbSession** | Per-worker sticky `Connection` + `PreparedStatement`; invalidate on SQLException / connection loss |
| **HealthMonitor** | Periodic probe **one path per POD pool**; publish UP / DOWN / DEGRADED |
| **ManagementBridge** | Serialize status events to existing management thread; alarm + broadcast |
| **MetricsRegistry** | Per-connection / per-POD / per-pool counters and gauges |

---

## 4. Connection & Threading Model

### 4.1 Sticky-per-worker (recommended — closest to current VNF)

Retain the mental model: each worker owns one DB connection for query processing.

```text
Startup (main thread):
  1. Load config: 3 × {instanceId, staticIp, port, credentials, role}
  2. PoolManager.createPools()          // 3 HikariCP DataSources
  3. ConnectionRouter.assignInitialAffinity(16 workers)  // ~equal across healthy PODs
  4. Each worker: borrow from assigned pool, prepare NP statement(s)
  5. Start HealthMonitor; wire ManagementBridge to management thread

Steady state:
  Worker_i:
    on query:
      execute prepared statement on sticky connection
      metrics.increment(queries, connectionId, podId)
    on SQLException / connection invalid:
      emit CONNECTION_DOWN(connectionId, podId, cause)
      close connection (return/discard to HikariCP)
      ask ConnectionRouter for next healthy pool (least-loaded)
      borrow + prepare again
      do NOT retry the failed business query

HealthMonitor:
  every T ms (e.g. 200–500 ms):
    for each POD pool:
      borrow (or use dedicated monitor connection)
      run probe; measure RTT
      if fail → count toward POD_DOWN
      if RTT > threshold → POD_DEGRADED
      emit events only on state transitions (edge-triggered)

Management thread:
  maintain DbInstanceState[3]
  on transition → alarm + broadcast to core modules
  ask ConnectionRouter to drain affinity from DOWN pods
```

**Fair distribution:** At assignment and on rebind, use **least-assigned healthy POD** (or weighted RR). Under uniform worker load, equal worker affinity ≈ equal query distribution across the three DB PODs.

### 4.2 Why not scan a shared connection list per query?

At 5000 qps with 16 workers, per-query list scanning + locking adds jitter and contention. Prefer:

- **O(1)** sticky connection on the worker (worker-local / ThreadLocal struct)
- Rebalance only on failure or health transition

### 4.3 Pool sizing formula

```text
workers = 16
pods = 3
monitor_conns = 1 per pod
per_pool_max = ceil(workers / pods) + monitor_headroom + reconnect_headroom
             ≈ 6 + 1 + 1 = 8

per_pool_min_idle ≈ ceil(workers / pods) = 6
```

HikariCP tips for low latency and fewer timeout scenarios:

| Property | Suggested | Why |
|----------|-----------|-----|
| `connectionTimeout` | 250–1000 ms | Fail fast; query is not retried anyway |
| `validationTimeout` | 200–500 ms | Bound validation wait |
| `keepaliveTime` | e.g. 30 s (HikariCP 5.x) | Avoid idle drops through CNF / firewalls |
| `maxLifetime` | Longer than peak idle; shorter than server idle drop | Controlled recycle |
| `leakDetectionThreshold` | On in lab; careful in prod | Overhead |

JDBC URL must use Multus static IP host:

```text
jdbc:postgresql://<MULTUS_IP>:<PORT>/<DB>?prepareThreshold=1&tcpKeepAlive=true
```

---

## 5. Load Balancing Strategy

### 5.1 Goals

1. Equal query distribution across **healthy** DB instances under steady state  
2. Immediate drain from DOWN instances  
3. Minimal cross-worker coordination on the query path  

### 5.2 Algorithm

**Initial / rebalance assignment (router, driven by management/health):**

```text
healthy = filter(pods, state == UP)   // optionally exclude DEGRADED if policy says so
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
  (hysteresis: wait N successful probes before UP)
```

### 5.3 Avoiding status / message loss

Interpreted as **status broadcast reliability** (not query retry):

- Health and connection events go through a **bounded queue** to the management thread
- Management thread is the **only** writer of `DbInstanceState`
- Broadcasts are **edge-triggered**; last-known state recoverable on subscriber reconnect
- Workers never block indefinitely on management; event offer with metric on drop (should be near-zero)

---

## 6. Health Monitoring & Alarms

### 6.1 Probe design

| Item | Recommendation |
|------|----------------|
| Probe SQL | `SELECT 1` (or lightweight NP probe if deeper check needed) |
| Frequency | 200–500 ms per POD (tune vs load) |
| Failure threshold | N consecutive failures (e.g. 2–3) before DOWN |
| Recovery threshold | M consecutive successes (e.g. 3–5) before UP (hysteresis) |
| Degraded | Probe succeeds but RTT above budget share (e.g. > 20–40 ms; tune in lab) |

Use **one probe path per POD pool**, not one probe for all PODs on a single connection.

### 6.2 Event types

```text
CONNECTION_DOWN   { workerId, podId, connectionId, errorCode, ts }
CONNECTION_UP     { workerId, podId, connectionId, ts }
POD_DOWN          { podId, reason, ts }
POD_DEGRADED      { podId, rttMs, ts }
POD_UP            { podId, ts }
POOL_EXHAUSTED    { podId, waitMs, ts }
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

**Timeouts:** Prefer failing the individual query quickly over long JDBC waits that stall a worker and reduce effective throughput.

---

## 8. Metrics (Minimum Set)

### Per connection (logical worker sticky id)

- `np_db_queries_total{pod,worker,connection}`
- `np_db_query_failures_total{pod,worker,reason}`
- `np_db_query_latency_ms` (histogram; track p50/p99 vs **200 ms** SLA)

### Per POD / pool

- `np_db_pool_active{pod}`
- `np_db_pool_idle{pod}`
- `np_db_pool_waiting{pod}`
- `np_db_connections_available{pod}`
- `np_db_connections_failed_total{pod}`
- `np_db_pod_state{pod}` (0=DOWN, 1=DEGRADED, 2=UP)
- `np_db_probe_rtt_ms{pod}`
- `np_db_queries_total{pod}` (LB fairness dashboards)

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
    degradedRttMs: 40
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
    - id: replica-2
      role: REPLICA
      jdbcUrl: jdbc:postgresql://10.10.1.3:5432/npdb?prepareThreshold=1&tcpKeepAlive=true
      maxPoolSize: 8
      minIdle: 6
```

Credentials via secrets mount / vault — not hardcoded. Multus static IPs are mandatory in `jdbcUrl` hosts.

---

## 10. Sequence: Query Path (Happy Path)

```text
Caller → Worker_i
Worker_i:
  ps = sticky.preparedStatement
  rs = ps.executeQuery(subscriber)
  map row → routing number
  metrics.ok(pod, worker, latency)
  return result
```

Illustrative budget inside module (SLA is **< 200 ms** end-to-end for the query):

| Step | Budget guidance |
|------|-----------------|
| Execute + fetch over Multus | Dominant cost; keep well under 200 ms |
| Module overhead (no lock scan) | Much less than 1 ms |
| Fail-fast on dead connection | Bounded by `connectionTimeout` / JDBC socket timeouts |

---

## 11. Sequence: Connection Loss

```text
Worker_i execute → SQLException (connection broken)
Worker_i:
  metrics.fail(...)
  emit CONNECTION_DOWN
  sticky.closeQuietly()
  pod = router.pickHealthy()
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
| Connection list of 16 | App-owned shared list scanned per query | **Sticky per worker** + HikariCP ownership |
| Per-query LB | Scan list each time | **Sticky**; rebalance on health events |
| Prepared statements | Precreate on shared list | Prepare on sticky borrow/rebind; driver cache assists |
| minIdle / max | 6 / 16 global | **~6 / 8 per pool** (tune to `max_connections`) |
| Monitor | Dummy query one conn per POD | Keep; add hysteresis + RTT degraded state |
| Retry | None | None (unchanged) |
| Status | Mgmt thread | Mgmt thread + edge-triggered queue |
| Latency SLA | (implied performance) | **< 200 ms** query latency; ≥ 5000 qps |

---

## 13. Performance Considerations (5000 qps / < 200 ms)

1. **No shared lock on the query path** — sticky connection in worker-local state.  
2. **Prepared statements** + Postgres JDBC server-prepare cache.  
3. **Fail-fast timeouts** so a hung POD does not stall workers beyond budget.  
4. **Warm pools** (`minimumIdle`) to avoid connect handshake during traffic.  
5. **tcpKeepAlive** + HikariCP keepalive to reduce silent dead connections on the CNF path.  
6. **Equal worker affinity** across PODs ≈ equal query distribution under uniform worker load.  
7. Size Postgres and OS for `3 × maxPoolSize × (number of client replicas)`.  
8. Keep thread model lean: 16 workers + management + one monitor — avoid extra thread pools.

Rough capacity check: 5000 qps / 16 workers ≈ **312 qps/worker** ≈ **3.2 ms average** spacing between queries per worker — compatible with **< 200 ms** latency if Multus RTT and DB execute time stay well below the SLA (typical NP point lookups should be far under 200 ms when healthy).

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

1. PoolManager + config (3 pools over static Multus IPs)  
2. WorkerDbSession sticky borrow / prepare / execute / invalidate  
3. ConnectionRouter LB + health gating  
4. HealthMonitor + ManagementStatusBridge  
5. Metrics + soak test at ≥ 5000 qps; validate p99 ≪ 200 ms  
6. Chaos: kill replica POD, primary role change, network partition — verify alarms, no stuck workers, fairness restored  

---

## 15. Open Decisions (product / ops)

1. **Read preference:** ANY (primary+replicas) vs REPLICAS_PREFERRED vs PRIMARY_ONLY given replication lag risk for NP routing correctness.  
2. **Multiple app client replicas:** confirm total connection budget vs Postgres `max_connections`.  
3. **Monitor interval vs CPU:** 250 ms × 3 probes — validate in lab.  
4. **Whether DEGRADED pods still accept sticky traffic** or are drain-only.  
5. **Primary failover:** static Multus IP assumption says IP is stable; confirm ops contract when primary **role** moves between PODs (IP vs role mapping).

---

## 16. Design Verdict

Keep from the proposed design:

- 16 workers + management thread  
- Multus static IP connectivity (mandatory)  
- No query retry in this module  
- Prepared statements on live connections  
- Per-POD monitoring + management alarms/broadcast  
- Metrics for queries/connection and connection availability  

Change the pooling model to:

- **Three HikariCP pools** (one per Multus static IP)  
- **Sticky-per-worker sessions** instead of a shared scanned connection list  
- **Health-gated least-loaded assignment** for fair LB across 3 DB PODs  
- **Fail-fast reconnect** so HikariCP re-establishes connections without double ownership  

This meets the CNF separation of app and DB PODs while preserving telco performance targets (**≥ 5000 qps**, **< 200 ms** query latency), load balancing, and reliable DB status maintenance.
