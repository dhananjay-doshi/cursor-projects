# High-Level Design: Number Portability DB Connection Management (`npdb-cnf`)

| Field | Value |
| --- | --- |
| Document | High-Level Design (HLD) |
| Module | Postgres Connection Management for Number Portability (NP) queries |
| Target runtime | Java (CNF / Kubernetes) |
| Connection pool | HikariCP |
| Database | PostgreSQL (1 Primary + 2 Replica), Multus static IPs |
| Status | **Draft — awaiting confirmation before implementation** |
| Audience | Architecture, Development, SRE / Ops |

---

## 1. Purpose and Scope

### 1.1 Purpose

Design a production-ready, telco-grade Java connection-management module that lets the Number Portability (NP) application client query subscriber and routing-number data stored in PostgreSQL, when the client and databases run on **separate Kubernetes pods**, while meeting:

- **Throughput**: ≥ **5,000 queries/sec**
- **Latency**: query path **&lt; 200 ms** (p99 target under normal load; see §10)
- **High availability**: survive single DB-pod loss without losing overall query capability
- **Mandatory networking**: JDBC connectivity only via **Multus static IPs** (non-negotiable)
- **Efficiency**: **PreparedStatements** on DB connections
- **Observability**: per-connection query counts, connection available/failed metrics, DB up/down events for alarms and core-module broadcast

### 1.2 In Scope

| Area | Included |
| --- | --- |
| Connection pools to 3 Postgres pods (HikariCP) | Yes |
| Fair query distribution across connections and DB instances | Yes |
| Worker / management / monitoring thread model | Yes |
| PreparedStatement lifecycle on connections | Yes |
| Connection up/down events → management → alarms / broadcast | Yes |
| Connection list maintenance after loss (no query retry in this module) | Yes |
| Metrics for queries and connection health | Yes |
| CNF-oriented config (static IPs, pool sizes, SQL) | Yes |

### 1.3 Out of Scope

| Area | Notes |
| --- | --- |
| Postgres HA / Patroni / replication topology | Assumed provided by platform |
| Schema / DDL for NP tables | Existing DB contract |
| Application-level retry / fallback after query failure | Owned by caller (explicit: **no retry in this module**) |
| Write / transactional NP updates | Design assumes **read-oriented** NP lookup queries |
| Multus CNI / network attachment definition | Platform concern; module consumes static IPs |
| Full application CNF packaging beyond this module’s API | Separate delivery |

### 1.4 Design Principles

1. **One HikariCP pool per DB pod** (JDBC URL = Multus static IP) — HikariCP does not load-balance across hosts.
2. **Application-level load balancer** over pools/connections — fair distribution is a first-class module responsibility.
3. **Fail fast on query failure** — mark connection unhealthy; do not re-issue the same query in this module.
4. **Separate data plane and control plane** — workers execute queries; management owns status and connection inventory; monitoring probes health.
5. **Predictable latency** — prefer pre-warmed pools, prepared statements, and bounded wait for connections over unbounded queueing.

---

## 2. Requirements Summary

### 2.1 Functional

| ID | Requirement |
| --- | --- |
| FR-01 | Query NP DB by subscriber number using PreparedStatements |
| FR-02 | Connect to each of 3 Postgres pods via fixed Multus static IP |
| FR-03 | Equally distribute queries across all three DB instances |
| FR-04 | Retain **16 worker threads** + **1 management thread** |
| FR-05 | Add **1 monitoring thread** sending a dummy/health query to **one connection per DB pod** |
| FR-06 | Workers (and monitor) emit connection up/down events to management |
| FR-07 | Management maintains DB instance status and broadcasts to other core modules; raises alarms |
| FR-08 | On query/connection failure: **do not retry**; mark loss; rebuild/maintain the 16-connection working set for continued LB |
| FR-09 | Expose metrics: queries sent per connection; connections failed / available |

### 2.2 Non-Functional

| ID | Requirement |
| --- | --- |
| NFR-01 | Telco-grade HA, production readiness |
| NFR-02 | ≥ 5,000 queries/sec |
| NFR-03 | Query latency &lt; 200 ms |
| NFR-04 | Low connection-timeout incidence; robust HikariCP re-establishment |
| NFR-05 | Minimize message/status loss for DB up/down broadcasting |

### 2.3 Constraints

| ID | Constraint |
| --- | --- |
| C-01 | Multus static IP connectivity is mandatory |
| C-02 | HikariCP is the connection pool framework |
| C-03 | Java application client |
| C-04 | PreparedStatements required |
| C-05 | Proposed pool sizing guidance: **minimumIdle = 6**, **maximumPoolSize = 16** (interpreted per architecture decision in §5.3) |

---

## 3. Current vs Target Architecture

### 3.1 Current (VNF — co-located)

```
┌─────────────────────────────────────────────┐
│                  VNF Node                   │
│  ┌─────────────────┐    ┌────────────────┐  │
│  │ App Client      │    │  Postgres DB   │  │
│  │ 16 workers      │───▶│  (local)       │  │
│  │ 1 management    │    │                │  │
│  │ Worker↔Conn map │    └────────────────┘  │
│  └─────────────────┘                        │
└─────────────────────────────────────────────┘
```

Characteristics:

- Client and DB on same node → low network risk, single DB endpoint
- Workers keep a sticky mapping to an assigned connection
- On termination, worker obtains a new pool connection and re-creates PreparedStatement
- Management thread broadcasts DB up/down to core modules

### 3.2 Target (CNF — separate pods)

```
                    ┌──────────────────────┐
                    │  App Client Pod      │
                    │  npdb-cnf module     │
                    │  16 workers          │
                    │  1 management        │
                    │  1 monitoring        │
                    │  3× HikariCP pools   │
                    └──────────┬───────────┘
           Multus static IPs   │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
┌───────────────┐      ┌───────────────┐      ┌───────────────┐
│ Postgres Pod  │      │ Postgres Pod  │      │ Postgres Pod  │
│ Primary       │      │ Replica-1     │      │ Replica-2     │
│ IP: A.B.C.D1  │      │ IP: A.B.C.D2  │      │ IP: A.B.C.D3  │
└───────────────┘      └───────────────┘      └───────────────┘
```

Characteristics:

- Network path is pod-to-pod over Multus; connection loss and failover become first-class
- Three readable endpoints; application must load-balance reads
- Sticky worker↔connection model is retained conceptually via a **Connection Registry**, backed by HikariCP for create/validate/re-establish

---

## 4. Logical Architecture

### 4.1 Component View

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Application Core Modules                         │
│         (alarms, status consumers, NP query request producers)           │
└───────────────▲──────────────────────────────────────────▲───────────────┘
                │ DB status broadcast                      │ query API
                │                                          │
┌───────────────┴──────────────────────────────────────────┴───────────────┐
│                         npdb-cnf  (this module)                          │
│                                                                          │
│  ┌────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │ NpQueryFacade  │  │ ManagementThread │  │ MonitoringThread         │  │
│  │ (entry API)    │  │ - DB status      │  │ - dummy query / pod      │  │
│  └───────┬────────┘  │ - connection set │  │ - up/down events         │  │
│          │           │ - alarms/events  │  └────────────┬─────────────┘  │
│          ▼           └────────▲─────────┘               │                │
│  ┌────────────────┐           │ events                  │                │
│  │ WorkerPool     │───────────┴─────────────────────────┘                │
│  │ (16 threads)   │                                                      │
│  └───────┬────────┘                                                      │
│          │ acquire / release / report                                    │
│          ▼                                                               │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │ ConnectionRegistry (16 active QuerySlots + LB policy)              │  │
│  └───────┬─────────────────────┬─────────────────────┬────────────────┘  │
│          ▼                     ▼                     ▼                   │
│  ┌───────────────┐     ┌───────────────┐     ┌───────────────┐           │
│  │ HikariPool-P  │     │ HikariPool-R1 │     │ HikariPool-R2 │           │
│  │ jdbc:…@IP1    │     │ jdbc:…@IP2    │     │ jdbc:…@IP3    │           │
│  └───────────────┘     └───────────────┘     └───────────────┘           │
└──────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Component Responsibilities

| Component | Responsibility |
| --- | --- |
| **NpQueryFacade** | Public API for NP lookup; submits work to workers / returns results; no direct JDBC |
| **WorkerPool (16)** | Execute PreparedStatement queries; pick slot via LB; emit failure/up events; **no query retry** |
| **ConnectionRegistry** | Maintains exactly the **working set of 16 query-ready connections** (slots), each bound to a pool/pod and a PreparedStatement; fair pick; rebuild on loss |
| **HikariPool × 3** | Per-pod pool: create, validate, idle management, re-establish TCP/JDBC sessions to static IP |
| **MonitoringThread** | Periodically health-checks **one connection (or lease) per DB pod**; publishes up/down |
| **ManagementThread** | Single owner of DB instance status FSM; updates registry membership; alarms; broadcasts to core modules |
| **MetricsCollector** | Counters/gauges: queries/connection, available/failed connections, pool stats, latency histograms |

### 4.3 Thread Model

| Thread | Count | Role |
| --- | --- | --- |
| Main (bootstrap) | 1 | Load config, create 3 Hikari pools, warm registry (16 slots), start threads |
| Worker | 16 | Data-plane query execution |
| Management | 1 | Control-plane: status, inventory, broadcast, alarms |
| Monitoring | 1 | Active health probes per DB pod |

**Concurrency rule:** Only Management mutates authoritative DB status and the structural membership of the 16-slot registry. Workers may mark a slot *suspect* and publish events; Management confirms and replaces.

---

## 5. Connection Pooling Design (HikariCP)

### 5.1 Why Three Pools

HikariCP binds a pool to a single JDBC URL / DataSource. Load balancing across Primary + 2 Replicas **cannot** be done inside one Hikari pool. Therefore:

```
PoolPrimary  → jdbc:postgresql://<MULTUS_IP_1>:5432/<db>
PoolReplica1 → jdbc:postgresql://<MULTUS_IP_2>:5432/<db>
PoolReplica2 → jdbc:postgresql://<MULTUS_IP_3>:5432/<db>
```

DNS/Service names are **not** used for the JDBC host when Multus static IP is required.

### 5.2 Bootstrap Sequence (Main Thread)

1. Load `npdb-cnf` configuration (IPs, credentials, pool params, SQL, timeouts).
2. Create three `HikariDataSource` instances.
3. Optionally `getConnection()` warm-up to force min idle creation.
4. Build **ConnectionRegistry** with 16 slots distributed across healthy pods (§5.4).
5. For each slot: obtain connection, create PreparedStatement(s), mark READY.
6. Start MonitoringThread, ManagementThread, WorkerPool.
7. Publish initial DB status (all known pods UP/DOWN based on warm-up).

### 5.3 Pool Sizing Decision

**Requirement text** suggests `minimumIdle = 6`, `maximumPoolSize = 16`. Applied naively **per pool** → up to 48 server connections, which may overwhelm Postgres `max_connections` and is unnecessary for 16 workers.

**Architectural decision (recommended):**

| Parameter | Per-pool value | Rationale |
| --- | --- | --- |
| `maximumPoolSize` | **6** | 16 workers ≈ 5–6 slots/pod; headroom for monitor + replacement |
| `minimumIdle` | **6** | Keep warm; aligns with “min idle 6” intent as **per pod** floor |
| Total max connections from client | **18** | 3 × 6; close to 16 active query slots + monitor/replacement |
| Active query slots in registry | **16** | Matches worker concurrency / proposed design |

Interpretation of the original “max 16”: treat as **aggregate active query connections** (registry size), not Hikari `maximumPoolSize` on a single pool.

**Alternative (if platform insists on literal Hikari max=16 per pool):** document Postgres capacity impact; still keep registry at 16 active slots and leave unused pool capacity for burst re-establishment only. **Default for this HLD is the recommended sizing above.**

### 5.4 Slot Distribution Across Pods

Target fair share while pods are healthy:

| Pod | Target slots (example) |
| --- | --- |
| Primary | 6 |
| Replica-1 | 5 |
| Replica-2 | 5 |
| **Total** | **16** |

When one pod is DOWN, rebalance remaining slots across healthy pods (e.g., 8 + 8) without exceeding each pool’s `maximumPoolSize`.

### 5.5 Recommended HikariCP Settings (Initial)

| Property | Suggested | Purpose |
| --- | --- | --- |
| `jdbcUrl` | Multus IP URL | Mandatory addressing |
| `maximumPoolSize` | 6 | See §5.3 |
| `minimumIdle` | 6 | Pre-warm |
| `connectionTimeout` | 250–500 ms | Fail fast vs 200 ms query budget |
| `validationTimeout` | 200–300 ms | Bound health validation |
| `idleTimeout` | 0 or high | Prefer stable long-lived conns for PreparedStatements |
| `maxLifetimeTime` | 30 min (tune) | Recycle to avoid silent server-side drops |
| `keepaliveTime` | 30–60 s | Reduce idle disconnects on CNF networks |
| `connectionTestQuery` | `SELECT 1` | Simple probe (or rely on JDBC `isValid`) |
| `autoCommit` | true (typical for RO lookup) | Avoid txn overhead |
| `readOnly` | true on replica pools | Safety + driver hints |
| `poolName` | `npdb-primary` / `npdb-r1` / `npdb-r2` | Metrics clarity |

Credentials via secrets (K8s Secret / mounted file), never hard-coded.

### 5.6 PreparedStatement Strategy

**Problem:** JDBC `PreparedStatement` is tied to a physical `Connection`. Pool checkout/return without care loses statement handles.

**Chosen approach — Long-lived leased connections in Registry (hybrid):**

1. Registry holds 16 **QuerySlots**.
2. Each READY slot holds:
   - reference to owning pool / pod id
   - a connection **leased from Hikari** for the slot lifetime (not returned until slot teardown)
   - one or more `PreparedStatement` instances created at slot init
3. Workers **borrow a slot** (not a raw Hikari checkout) under LB + availability flags, execute, release slot lock.
4. On failure/closed connection: worker marks slot FAILED → event → Management closes/abandons connection (`close()` returns it broken to Hikari) → acquires replacement from appropriate pool → re-prepares statements → marks READY.

**Why this fits requirements:**

- Preserves current VNF mental model (worker uses prepared connection)
- Satisfies “list of 16 connections with pre-defined PreparedStatements”
- Still uses Hikari for creation, validation, and re-establishment
- Avoids prepare-per-query cost under 5k qps

**Complementary:** enable driver / Hikari prepared statement caching as belt-and-suspenders for any short-lived checkouts used by the MonitoringThread.

### 5.7 Monitoring vs Registry Connections

Prefer **not** stealing a worker slot for health checks:

- MonitoringThread checks out a **short-lived** connection from each pool (or uses HikariMXBean + occasional `SELECT 1`), then returns it.
- This keeps 16 slots dedicated to data plane and matches “dummy query to one connection per DB pod.”

---

## 6. Query Processing and Load Balancing

### 6.1 Query Path (Happy Path)

```
NP request → NpQueryFacade → Worker thread
    → LoadBalancer.selectSlot(policy)
    → Slot.execute(PreparedStatement, bind subscriber number)
    → map ResultSet → NP response DTO
    → release slot
    → increment metrics
```

### 6.2 Load-Balancing Policy

**Primary policy: Weighted Round-Robin across READY slots**, with pod-level fairness.

1. Maintain atomic cursor over READY slots.
2. Skip slots that are FAILED / DRAINING / locked.
3. Optional secondary preference: **least in-flight** if a slot is busy (mutex), scan next.
4. Ensure over time query counts per pod converge (monitor with metrics).

**Equal distribution expectation:** With 16 slots ≈ evenly mapped to 3 pods, round-robin on slots ≈ equal distribution across DB instances. When a pod drops, redistribution of slots restores balance across survivors.

### 6.3 Worker–Slot Interaction

Do **not** permanently pin worker-N to connection-N (that reduces fairness when some workers are hotter). Instead:

- Any worker may use any READY slot
- Short critical section: acquire slot lock → execute → unlock
- 16 workers + 16 slots ⇒ high parallelism with minimal wait if locks are per-slot

This meets “scan list and pick available connection” while improving fairness vs sticky mapping.

### 6.4 Failure Path (No Retry)

```
execute() throws / connection invalid
    → metrics: query_failed++, connection_failed++
    → mark slot SUSPECT/FAILED (local)
    → publish ConnectionDownEvent(podId, slotId, cause) to Management queue
    → return failure to caller (no re-query)
    → Management asynchronously replaces slot and may flip pod status
```

Application core modules own any higher-level retry / alternate routing.

### 6.5 Latency Budget (Indicative)

| Segment | Budget |
| --- | --- |
| Queue / worker handoff | ≤ 10 ms |
| Slot acquire | ≤ 5 ms |
| Postgres execute + network (Multus) | ≤ 150 ms |
| Result mapping | ≤ 10 ms |
| **Total** | **&lt; 200 ms** |

`connectionTimeout` must stay well below remaining budget so workers fail fast rather than pile up.

---

## 7. Status Management, Alarms, and Broadcast

### 7.1 DB Instance State Machine (per pod)

```
        bootstrap OK
   UNKNOWN ──────────► UP ◄──────────────┐
      │                │                 │
      │ probe/query    │ consecutive     │ recovery probes /
      │ fail           │ failures        │ successful queries
      ▼                ▼                 │
      DOWN ◄──────── FAILED_PROBE        │
      │                                  │
      └──────────────────────────────────┘
```

**UP:** Pod accepts queries; slots may be allocated.  
**DOWN:** No new slots; existing slots drained/failed; alarms raised; broadcast DOWN.  
**UNKNOWN:** Only during startup until first successful probe or warm-up.

### 7.2 Event Sources

| Source | Event |
| --- | --- |
| Worker | Query-time connection loss / SQLException indicating broken connection |
| MonitoringThread | Dummy query success/failure per pod |
| Bootstrap | Initial warm-up success/failure |

### 7.3 Management Thread Duties

1. Consume events from a **bounded, non-blocking ingress** (e.g., `ArrayBlockingQueue` / Disruptor) sized to avoid event loss under burst; on overflow: prefer coalescing per-pod status over dropping DOWN events.
2. Update pod FSM with hysteresis (e.g., N consecutive failures → DOWN; M successes → UP) to avoid flapping.
3. Rebuild ConnectionRegistry membership to keep **16 READY slots** when capacity allows.
4. Raise alarms (interface to existing alarm module).
5. Broadcast DB status snapshots to core modules (versioned status object: podId → state, timestamp, reason).

### 7.4 Avoiding Status / Message Loss

- Single Management thread = serialized status authority (no conflicting broadcasts)
- Coalesce duplicate UP/UP events; never coalesce away a DOWN without processing
- Periodic **status heartbeat broadcast** (e.g., every 1–2 s) so consumers converge even if one event was missed
- Persist nothing required for correctness; in-memory + replay heartbeat is enough for this module

---

## 8. Monitoring Thread Design

| Aspect | Design |
| --- | --- |
| Frequency | Configurable; default 1–2 s |
| Scope | One health query **per DB pod** per cycle |
| Method | Short Hikari checkout + `SELECT 1` (or lightweight NP-safe SQL) |
| Timeout | Strict socket/query timeout &lt; interval |
| On failure | Emit `PodProbeFailed` → Management |
| On success after DOWN | Emit `PodProbeSucceeded` → Management |
| Isolation | Must not block workers; use own connections from pool headroom |

---

## 9. Metrics

### 9.1 Required Metrics

| Metric | Type | Labels |
| --- | --- | --- |
| `npdb_queries_total` | Counter | `pod_id`, `slot_id`, `result=success\|failure` |
| `npdb_queries_per_connection` | Counter | `pod_id`, `slot_id` |
| `npdb_connections_available` | Gauge | `pod_id` |
| `npdb_connections_failed` | Counter | `pod_id`, `reason` |
| `npdb_slots_ready` | Gauge | `pod_id` |
| `npdb_pod_status` | Gauge | `pod_id` (1=UP,0=DOWN) |

### 9.2 Recommended Additional Metrics

| Metric | Type | Purpose |
| --- | --- | --- |
| `npdb_query_latency_ms` | Histogram | Validate &lt; 200 ms |
| `npdb_slot_acquire_wait_ms` | Histogram | Detect contention |
| `hikaricp_*` | Hikari Micrometer/JMX | Pool pending, active, timeout count |
| `npdb_status_events_total` | Counter | UP/DOWN volume |
| `npdb_status_queue_depth` | Gauge | Event backlog / loss risk |

Export via Micrometer → Prometheus (or existing telco telemetry bus).

---

## 10. Performance Considerations (5k qps / &lt;200 ms)

### 10.1 Capacity Sketch

- 5,000 qps / 16 workers ≈ **312.5 qps per worker**
- Average service time must stay ≈ **3.2 ms** per query for CPU pipeline saturation math; network+DB will dominate — ensure DB indexes on subscriber number and local Multus RTT remain low
- 16 concurrent in-flight queries matches 16 slots; Postgres and replicas must sustain that concurrency comfortably

### 10.2 Key Optimizations

1. Pre-created PreparedStatements on warm connections  
2. `minimumIdle` equals pool max → no cold-start checkout stalls  
3. Hikari `keepaliveTime` + `maxLifetimeTime` to reduce surprise disconnects  
4. Fail-fast `connectionTimeout` to protect latency SLO  
5. Lock-free / low-lock LB cursor; per-slot mutex only during execute  
6. Avoid synchronized logging on query path  
7. Prefer binary Transfer / efficient ResultSet mapping for NP fields  

### 10.3 Timeout / Re-establishment

| Scenario | Behavior |
| --- | --- |
| TCP reset mid-query | Fail query to caller; replace slot via Management |
| Pool exhausted | Should be rare (slots ≤ pool capacity); monitor timeouts |
| Pod DOWN | Drain slots; redistribute; alarm; continue on remaining pods |
| Pod recovery | Monitor detects UP; Management adds slots back toward fair share |

---

## 11. Configuration Model

Logical configuration (names illustrative):

```yaml
npdb:
  databases:
    - id: primary
      host: "<MULTUS_IP_1>"   # mandatory static IP
      port: 5432
      database: npdb
      role: PRIMARY
    - id: replica-1
      host: "<MULTUS_IP_2>"
      port: 5432
      database: npdb
      role: REPLICA
    - id: replica-2
      host: "<MULTUS_IP_3>"
      port: 5432
      database: npdb
      role: REPLICA
  pool:
    maximumPoolSize: 6
    minimumIdle: 6
    connectionTimeoutMs: 300
    validationTimeoutMs: 250
    keepaliveTimeMs: 30000
    maxLifetimeTimeMs: 1800000
  registry:
    activeSlots: 16
  threads:
    workers: 16
    monitoringIntervalMs: 1000
  sql:
    npLookup: "SELECT ... FROM ... WHERE subscriber_number = ?"
    health: "SELECT 1"
  status:
    failThreshold: 3
    recoverThreshold: 2
    heartbeatBroadcastMs: 1000
```

---

## 12. Public API (Conceptual)

```text
NpDbClient.init(config) -> void
NpDbClient.lookup(subscriberNumber) -> NpLookupResult | Error
NpDbClient.getDbStatus() -> DbStatusSnapshot
NpDbClient.registerStatusListener(listener) -> void
NpDbClient.shutdown() -> void
```

Internal types (illustrative): `QuerySlot`, `PodId`, `ConnectionEvent`, `DbStatusSnapshot`.

Exact Java package layout deferred to Low-Level Design / implementation (pending confirmation).

---

## 13. Deployment View (CNF)

```
Namespace: np (example)
┌─────────────────────┐
│ app-client Deployment│  replicas per site capacity plan
│  - Multus NAD        │
│  - env/secret: DB    │
│  - npdb-cnf lib      │
└─────────────────────┘
          │ Multus L2/L3
┌─────────┴─────────┬─────────────────┐
│ pg-primary Pod    │ pg-replica Pods │
│ Multus IP static  │ Multus IP static│
└───────────────────┴─────────────────┘
```

- NetworkPolicy must allow client pod → Postgres ports on Multus IPs  
- Postgres `pg_hba.conf` / users must allow client Multus CIDR  
- Resource requests/limits sized for 5k qps path (CPU for 16 workers)

---

## 14. Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Replica lag returns stale NP data | Wrong routing number | Document read-your-replicas consistency; optional primary-only mode for critical queries (config flag) |
| Literal maxPoolSize=16 × 3 | DB connection exhaustion | Adopt §5.3 recommended sizing; align with DBA |
| Long-lived leased connections vs pure pool | Harder to share under imbalance | Management rebalances slots; monitor keeps pools honest |
| Status event queue overflow | Missed alarms | Coalesce + heartbeat broadcast |
| Multus / CNI flaps | Mass connection loss | Fast fail, slot rebuild, pod DOWN hysteresis |
| PreparedStatement invalid after server restart | Query errors | On specific SQLStates, invalidate slot and rebuild |
| Uneven worker load | Uneven pod QPS | Slot-based RR, not sticky worker mapping |

---

## 15. Open Decisions (Confirm Before Coding)

| # | Topic | Options | HLD Default |
| --- | --- | --- | --- |
| OD-01 | Hikari `maximumPoolSize` | 6 per pool (recommended) vs 16 per pool (literal req) | **6 per pool** |
| OD-02 | Reads on Primary | Include in LB vs replicas-only | **Include all 3** (per requirement) |
| OD-03 | Sticky worker↔connection | Sticky vs shared slot RR | **Shared slot RR** |
| OD-04 | Metrics backend | Micrometer/Prometheus vs legacy counters | **Micrometer** if app standard allows |
| OD-05 | Status listener API | Push callback vs pull + heartbeat | **Push + heartbeat** |
| OD-06 | Consistency mode | Always LB vs optional primary-preferred | **Always LB**; add flag later if needed |

---

## 16. Implementation Phases (After Confirmation)

> **No code will be generated until this HLD is confirmed.**

| Phase | Deliverable |
| --- | --- |
| P0 | Confirm OD-01…OD-06; freeze config schema |
| P1 | Module skeleton, config loader, 3× HikariDataSource factory |
| P2 | ConnectionRegistry + PreparedStatement slot lifecycle |
| P3 | Worker pool + NpQueryFacade + fail-fast path |
| P4 | MonitoringThread + ManagementThread + status broadcast/alarms SPI |
| P5 | Metrics + load-balancing verification tests |
| P6 | Chaos tests: kill replica, network delay, connection reset; latency/throughput soak |

---

## 17. Test Strategy (Design Level)

| Test type | Intent |
| --- | --- |
| Unit | LB fairness, FSM hysteresis, slot rebuild |
| Integration | 3 Postgres testcontainers / IPs; PreparedStatement path |
| Performance | 5k qps soak; p99 &lt; 200 ms |
| Failure injection | Pod DOWN mid-load; verify no query retry; metrics/alarms |
| Config negative | Bad IP, auth fail, pool timeout behavior |

---

## 18. Document History

| Version | Date | Author | Notes |
| --- | --- | --- | --- |
| 0.1 | 2026-07-13 | Principal Software Architect (npdb-cnf) | Initial HLD from requirements; awaiting confirmation |

---

## 19. Confirmation Checkpoint

Please review and confirm:

1. Overall architecture (3 Hikari pools + 16-slot registry + 16 workers + management + monitoring)  
2. Pool sizing default (**6 max / 6 min idle per pod**, 16 active slots)  
3. Shared slot round-robin (vs sticky worker–connection mapping)  
4. No query retry inside this module  
5. Multus static IP–only JDBC URLs  
6. Open decisions OD-01…OD-06  

**Upon your confirmation, implementation (Java module code) can proceed from Phase P1.**  
**Until then, this repository change is limited to `npdb-cnf/HLD.md` only.**
