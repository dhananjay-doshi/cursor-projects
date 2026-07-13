# High-Level Design: Number Portability DB Connection Management (`npdb-cnf`)

| Field | Value |
| --- | --- |
| Document | High-Level Design (HLD) |
| Module | Postgres Connection Management for Number Portability (NP) queries |
| Target runtime | Java (CNF / Kubernetes) |
| Connection pool | HikariCP |
| Database | PostgreSQL (1 Primary + 2 Replica), Multus static IPs |
| Status | **Draft v0.3 — query response time SLO confirmed (&lt; 20 ms); awaiting remaining OD confirmations** |
| Audience | Architecture, Development, SRE / Ops |
| Related | [`DESIGN_REVIEW.md`](./DESIGN_REVIEW.md) |

---

## 1. Purpose and Scope

### 1.1 Purpose

Design a production-ready, telco-grade Java connection-management module that lets the Number Portability (NP) application client query subscriber and routing-number data stored in PostgreSQL, when the client and databases run on **separate Kubernetes pods**, while meeting:

- **Throughput**: ≥ **5,000 queries/sec**
- **Latency**: query response time **&lt; 20 ms** (**confirmed** requirement)
- **High availability**: survive single DB-pod loss / restart without losing overall query capability
- **Equal load distribution** across healthy PostgreSQL instances, including during and after a server restart
- **No NP message failure** on infrastructure errors when at least one other pool is healthy (intra-request pool failover — §20)
- **Mandatory networking**: JDBC connectivity only via **Multus static IPs** (non-negotiable)
- **Efficiency**: PreparedStatements via PostgreSQL JDBC driver server-side cache
- **Observability**: per-pool / per-connection metrics; DB up/down events for alarms and SLP broadcast

### 1.2 In Scope

| Area | Included |
| --- | --- |
| Connection pools to 3 Postgres pods (HikariCP) | Yes |
| Fair query distribution across healthy DB instances (incl. restart) | Yes |
| Worker / management / monitoring thread model | Yes |
| Borrow/return connections + driver PreparedStatement cache | Yes |
| Instant quarantine + intra-request pool failover on infra failure | Yes |
| Planned drain / gated recovery for Postgres restart | Yes |
| Connection up/down events → management → alarms / SLP broadcast | Yes |
| Metrics for queries and connection health | Yes |
| CNF-oriented config (static IPs, pool sizes, SQL) | Yes |

### 1.3 Out of Scope

| Area | Notes |
| --- | --- |
| Postgres HA / CNPG / replication topology | Assumed provided by platform |
| Schema / DDL for NP tables | Existing DB contract |
| Business-level NP retransmission after definitive failure | Owned by caller / SLP |
| Unbounded retry on the same pool | Not allowed |
| Multus CNI / NAD | Platform concern; module consumes static IPs |
| Full application CNF packaging beyond this module’s API | Separate delivery |

### 1.4 Design Principles

1. **One HikariCP pool per DB pod** (JDBC URL = Multus static IP).
2. **Application-level RR** over **healthy** pools only (common DB connector).
3. **Borrow/return** — workers never permanently own connections.
4. **Fail closed** — atomic quarantine removes a bad pool from RR immediately.
5. **Intra-request pool failover** on infrastructure errors (bounded; max 2 alternates).
6. **Data plane ≠ control plane** — workers query; management owns alarms/SLP; monitor recovers.
7. **Predictable latency** — warm pools, short NP timeouts, never select DOWN/DRAINING pools.

---

## 2. Requirements Summary

### 2.1 Functional

| ID | Requirement |
| --- | --- |
| FR-01 | Query NP DB by subscriber number using PreparedStatements |
| FR-02 | Connect to each of 3 Postgres pods via fixed Multus static IP |
| FR-03 | Equally distribute queries across all **UP** DB instances (incl. after restart) |
| FR-04 | **16 worker threads** + **1 management** + **1 monitoring** |
| FR-05 | Monitor each pool (one connection + `SELECT 1`), default interval **5 s** |
| FR-06 | Workers and monitor emit up/down events to management |
| FR-07 | Management maintains pool status, alarms, broadcasts to SLP(s) |
| FR-08 | On NP infra failure: quarantine pool; failover to next UP pool (max 2 alternates) |
| FR-08b | Service OAM may use up to 2 alternate-pool attempts |
| FR-09 | Metrics: queries/pool/connection, failed/available, transitions |
| FR-10 | Postgres restart: equal LB on survivors; no NP message loss if ≥1 other pool healthy |
| FR-11 | App starts if DB down (`initializationFailTimeout=0`); Hikari reconnects in background |

### 2.2 Non-Functional

| ID | Requirement |
| --- | --- |
| NFR-01 | Telco-grade HA, production readiness |
| NFR-02 | ≥ 5,000 queries/sec |
| NFR-03 | Query response time **&lt; 20 ms** (confirmed) |
| NFR-04 | Low connection-timeout incidence; robust Hikari re-establishment |
| NFR-05 | Minimize status-message loss for SLP broadcast |
| NFR-06 | No NP message failure on infra errors when another pool can serve |

### 2.3 Constraints

| ID | Constraint |
| --- | --- |
| C-01 | Multus static IP connectivity is mandatory |
| C-02 | HikariCP is the connection pool framework |
| C-03 | Java application client |
| C-04 | PreparedStatements required |
| C-05 | Stakeholder sizing: **minimumIdle = 17**, **maximumPoolSize = 17** per pool |

---

## 3. Current vs Target Architecture

### 3.1 Current (VNF — co-located)

- 16 workers, each owning a dedicated connection + PreparedStatement
- 1 management thread broadcasts DB status to SLP(s)
- Local DB access; single endpoint

### 3.2 Target (CNF — separate pods)

```
                    ┌──────────────────────┐
                    │  App Client Pod      │
                    │  DB connector        │
                    │  16 workers          │
                    │  1 management        │
                    │  1 monitoring        │
                    │  3× HikariCP (max17) │
                    └──────────┬───────────┘
           Multus static IPs   │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
┌───────────────┐      ┌───────────────┐      ┌───────────────┐
│ PG Primary    │      │ PG Replica-1  │      │ PG Replica-2  │
│ Multus IP #1  │      │ Multus IP #2  │      │ Multus IP #3  │
└───────────────┘      └───────────────┘      └───────────────┘
```

---

## 4. Logical Architecture

### 4.1 Component View

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         SLP / Core Modules                               │
└───────────────▲──────────────────────────────────────────▲───────────────┘
                │ status broadcast                         │ NP query API
┌───────────────┴──────────────────────────────────────────┴───────────────┐
│                    npdb-cnf / common DB connector                        │
│  ┌────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │ Query + Failover│  │ ManagementThread │  │ MonitoringThread (5s)   │  │
│  │ Facade          │  │ pool FSM/alarms  │  │ SELECT 1 per pool       │  │
│  └───────┬────────┘  └────────▲─────────┘  └────────────┬─────────────┘  │
│          │                    │ DBEventQueue            │                │
│  ┌───────▼────────┐           │                         │                │
│  │ WorkerPool ×16 │───────────┴─────────────────────────┘                │
│  └───────┬────────┘                                                      │
│          │ RR select UP pool → getConnection → PS execute → close()      │
│  ┌───────▼────────────────────────────────────────────────────────────┐  │
│  │ HealthyPoolSelector (atomic eligibility mask + RR cursor)          │  │
│  └───┬──────────────────────┬──────────────────────┬──────────────────┘  │
│      ▼                      ▼                      ▼                     │
│  Hikari Primary         Hikari R1              Hikari R2                 │
│  (Multus IP, max 17)    (Multus IP, max 17)    (Multus IP, max 17)       │
└──────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Responsibilities

| Component | Responsibility |
| --- | --- |
| **DB connector / Facade** | Healthy-pool RR, borrow/execute/return, intra-request failover |
| **WorkerPool (16)** | Execute NP lookups; emit infra-failure events; never pin connections |
| **HealthyPoolSelector** | RR among UP only; honor quarantine/DRAINING instantly |
| **HikariPool × 3** | Connection lifecycle and background re-establishment |
| **MonitoringThread** | Probe all pools; drive gated recovery |
| **ManagementThread** | Authoritative FSM, alarms, SLP broadcast, stats |
| **MetricsCollector** | QPS, per-pool counts, failures, transitions, latency |

### 4.3 Thread Model

| Thread | Count | Role |
| --- | --- | --- |
| Main | 1 | Bootstrap pools (`initializationFailTimeout=0`), start threads |
| Worker | 16 | Data plane |
| Management | 1 | Control plane |
| Monitoring | 1 | Health / recovery |

Workers may **atomically quarantine** a pool in the selector; Management confirms FSM + SLP side effects.

---

## 5. Connection Pooling Design (HikariCP)

### 5.1 Three Pools (Mandatory)

```
PoolPrimary  → jdbc:postgresql://<MULTUS_IP_1>:5432/m7np_db?connectTimeout=...&socketTimeout=...&tcpKeepAlive=true
PoolReplica1 → jdbc:postgresql://<MULTUS_IP_2>:5432/m7np_db?...
PoolReplica2 → jdbc:postgresql://<MULTUS_IP_3>:5432/m7np_db?...
```

No DNS/Service hostname for the JDBC host when Multus static IP is required.

### 5.2 Bootstrap

1. Load config (IPs, secrets, pool/timeouts, SQL).
2. Create three `HikariDataSource` with `initializationFailTimeout = 0` (process stays up if DB down).
3. Warm pools when reachable; publish initial status.
4. Start Monitoring, Management, Workers.
5. Hikari continues background connection establishment for DOWN pools.

### 5.3 Pool Sizing (Stakeholder-Aligned)

| Parameter | Per-pool | Rationale |
| --- | --- | --- |
| `maximumPoolSize` | **17** | 16 workers can all fail over onto one surviving pool + 1 monitor |
| `minimumIdle` | **17** | Fully warm; no idle/max gap |
| Client total max | **51** | 17 × 3 (correct the attached doc’s “48” if max=17) |

**DBA check required** against Postgres `max_connections` (and other clients).

Service OAM may use smaller pools (e.g., min=max=3) per stakeholder note.

### 5.4 Recommended Hikari / JDBC Settings (NP Path)

| Property | Stakeholder | HLD NP recommendation | Notes |
| --- | --- | --- | --- |
| `maximumPoolSize` / `minimumIdle` | 17 / 17 | **17 / 17** | Adopt |
| `keepaliveTime` | 30000 ms | **30000 ms** | App monitor every 5 s |
| `maxLifetimeTime` | 1800000 ms | **1800000 ms** | Recycle quietly |
| `initializationFailTimeout` | 0 | **0** | CNF boot resilience |
| `validationTimeout` | 250 ms | **250 ms** | &lt; connectionTimeout |
| `connectionTimeout` | 1000 ms | **≤ 20–50 ms (NP)** | Fail fast → failover; 1000 ms breaks &lt;20 ms SLO |
| `registerMbeans` | true | **true** | Ops visibility |
| Statement query timeout | 2 s | **≤ 15–20 ms (NP)** | Align to latency SLO |
| JDBC `socketTimeout` | 1 s | **Align to NP query budget** | Else workers block during restart |
| JDBC `tcpKeepAlive` | true | **true** | Multus path hygiene |

OAM path may keep looser timeouts than NP.

### 5.5 PreparedStatement Strategy

Adopt stakeholder approach (no app-owned PS cache):

```
prepareThreshold=1              # prefer 1 for NP hot path (stakeholder had 3)
preparedStatementCacheQueries=20
preparedStatementCacheSizeMiB=5
```

Borrow connection → use `PreparedStatement` → `close()` returns connection to Hikari. Driver recreates server-side prepared statements after connection replacement.

---

## 6. Query Processing and Load Balancing

### 6.1 Happy Path

```
NP request → Worker
  → HealthyPoolSelector.nextUpPool()          // round-robin
  → dataSource.getConnection()
  → PreparedStatement execute (query timeout)
  → map ResultSet
  → connection.close()                        // return to pool
  → metrics++
```

### 6.2 Equal Distribution Rules

| Condition | RR set | Expected share |
| --- | --- | --- |
| 3 pools UP | P, R1, R2 | ~33% / 33% / 33% |
| 1 pool DRAINING/DOWN | 2 remaining | ~50% / 50% |
| 1 pool UP | sole survivor | 100% |
| Recovered pool gated to UP | all UP again | returns to ~33% / 33% / 33% |

Workers **never** call `getConnection()` on non-UP pools → avoids timeout storms.

### 6.3 Intra-Request Failover (No Message Failure)

On **infrastructure** failures only (socket closed, reset, broken pipe, acquire timeout, connection SQLStates):

```
attempt = 0
loop:
  pool = nextUpPool()
  if pool == null → return failure to SLP
  try borrow + execute + return success
  catch infraError:
    quarantine(pool)                 // atomic: remove from RR immediately
    enqueue DbEvent(DOWN/DRAINING)
    attempt++
    if attempt > maxAlternatePools (2):
      return failure to SLP
    // else continue loop on next UP pool (same NP request)
```

- **Not** a business retry loop on one pool.
- Same capacity math as OAM (up to 3 pools tried).
- If ≥1 other pool is healthy, NP message succeeds during a single-pod restart.

Business SQL errors (syntax, no row policy, etc.) fail once — no failover.

---

## 7. Pool Status Management

### 7.1 State Machine (Extended)

```
                 planned drain / first infra fail
   UP ──────────────────────► DRAINING ──────────────► DOWN
   ▲                              │                      │
   │                         in-flight drain             │ monitor
   │                              ▼                      ▼
   │                         (no new borrows)      RECOVERING
   │                                                     │
   └──────── consecutive probes OK + pool warmed ────────┘
```

| State | RR eligible | Purpose |
| --- | --- | --- |
| UP | Yes | Full weight |
| DRAINING | No | Quarantine / planned restart; finish in-flight |
| DOWN | No | Confirmed unavailable |
| RECOVERING | No | Probes succeeding; warm before rejoin |

Management is authoritative for alarms/SLP. Selector quarantine may enter DRAINING without waiting on the event queue.

### 7.2 Event Path

```
Workers / Monitoring → BlockingQueue<DBEvent> → Management
  → update FSM → alarm → broadcast SLP → stats
```

Coalesce duplicate UPs; never drop the first DOWN. Periodic status heartbeat (1–2 s) prevents SLP divergence.

### 7.3 Detection Sources

| Source | Role |
| --- | --- |
| Worker infra failure | Fast path quarantine (primary during load) |
| Monitoring every 5 s | Detect idle outages + drive recovery |
| Admin / ops drain API | Planned restart with zero intentional loss |

---

## 8. Monitoring Thread

| Aspect | Design |
| --- | --- |
| Interval | **5 s** default (configurable) |
| Action | `getConnection()` + `SELECT 1` on **each** pool (UP and DOWN) |
| Failure | Event → Management (DOWN/DRAINING) |
| Success while DOWN/RECOVERING | Count toward gated UP |
| Isolation | Uses pool headroom (17th connection intent); must not block workers |

Attached note (“single query marks up/down”) is **accepted for DOWN**, **rejected for UP** without gating (§20.3).

---

## 9. Metrics

| Metric | Type | Labels / notes |
| --- | --- | --- |
| `npdb_queries_total` | Counter | `pool_id`, `result` |
| `npdb_queries_per_pool` | Counter | Equal-LB verification |
| `npdb_queries_per_connection` | Counter | pool + connection id (if required) |
| `npdb_connections_available` | Gauge | per pool (Hikari idle/active) |
| `npdb_connection_failures` | Counter | pool, reason |
| `npdb_acquire_timeouts` | Counter | pool |
| `npdb_failover_total` | Counter | from_pool, to_pool |
| `npdb_pool_status` | Gauge | pool → state enum |
| `npdb_query_latency_ms` | Histogram | validate &lt;20 ms |
| Hikari JMX / Micrometer | — | active/idle/pending/timeout |

---

## 10. Performance (5k qps / &lt;20 ms)

### 10.1 Confirmed Latency SLO

**Query response time must be &lt; 20 ms** (end-to-end NP lookup through the DB connector under normal load).

Indicative budget for a single successful attempt (no failover):

| Segment | Budget |
| --- | --- |
| Pool select + `getConnection()` | ≤ 2–3 ms |
| Postgres execute + Multus RTT | ≤ 12–15 ms |
| Result mapping + return to pool | ≤ 2 ms |
| **Total** | **&lt; 20 ms** |

If intra-request failover occurs, each failed attempt must fail fast (short `connectionTimeout` / statement timeout) so the successful alternate attempt can still finish within the overall budget where possible; measure p99 with and without failover in soak tests.

### 10.2 Capacity Sketch

- 5,000 qps / 16 workers ≈ 312 qps/worker → average service time must stay near **~3 ms**; &lt;20 ms is the hard ceiling / p99 budget.
- Warm `minimumIdle=17` avoids cold acquire.
- **Critical:** NP `connectionTimeout` and statement/socket timeouts must be **≪ 20 ms**, not the stakeholder 1–2 s values.
- Index subscriber number; keep Multus RTT low.
- Avoid synchronized logging on query path.

---

## 11. Configuration Model (Illustrative)

```yaml
npdb:
  databases:
    - id: primary
      host: "<MULTUS_IP_1>"
      port: 5432
      database: m7np_db
      role: PRIMARY
    - id: replica-1
      host: "<MULTUS_IP_2>"
      ...
    - id: replica-2
      host: "<MULTUS_IP_3>"
      ...
  pool:
    maximumPoolSize: 17
    minimumIdle: 17
    connectionTimeoutMs: 50          # NP; tune in soak
    validationTimeoutMs: 250
    keepaliveTimeMs: 30000
    maxLifetimeTimeMs: 1800000
    initializationFailTimeoutMs: 0
  jdbc:
    connectTimeoutSec: 1
    socketTimeoutMs: 20              # align to SLO
    tcpKeepAlive: true
    prepareThreshold: 1
  threads:
    workers: 16
    monitoringIntervalMs: 5000
  failover:
    maxAlternatePools: 2             # NP + OAM
  status:
    recoverProbeSuccesses: 3
    requireMinIdleBeforeUp: true
    heartbeatBroadcastMs: 1000
  sql:
    npLookup: "SELECT ... WHERE subscriber_number = ?"
    health: "SELECT 1"
```

---

## 12. Public API (Conceptual)

```text
NpDbClient.init(config)
NpDbClient.lookup(subscriberNumber) -> result | error
NpDbClient.drainPool(poolId)                 # planned restart
NpDbClient.getDbStatus() -> snapshot
NpDbClient.registerStatusListener(listener)
NpDbClient.shutdown()
```

---

## 13. Deployment View (CNF)

- App client Deployment with Multus NAD; secrets for DB credentials.
- Three Postgres pods with static Multus IPs.
- NetworkPolicy + `pg_hba` allow client Multus CIDR.
- Guardrail: avoid draining/restarting more than one NP DB pod at a time unless forced.

---

## 14. Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Restart failure window while pool still UP | Atomic quarantine + intra-request failover (§20) |
| Early UP after restart | RECOVERING + N probes + minIdle warm |
| Timeouts ≫ latency SLO | NP-specific short timeouts |
| 51 client connections | DBA approval of `max_connections` |
| Replica lag | Document consistency; optional primary-preferred later |
| Event queue burst | Coalesce; never drop first DOWN; heartbeat |
| Rolling restart of all 3 | Alarm if UP count &lt; 2; ops runbook |

---

## 15. Open Decisions (Confirm Before Coding)

| # | Topic | Options | HLD Default |
| --- | --- | --- | --- |
| OD-01 | Pool size | 17 per pool vs smaller | **17 / 17** (stakeholder) |
| OD-02 | Include Primary in RR | Yes / replicas-only | **Yes — all 3** |
| OD-03 | Connection model | Borrow/return vs leased slots | **Borrow/return** |
| OD-04 | Metrics backend | Micrometer / JMX / legacy | **JMX + Micrometer if available** |
| OD-05 | Status API | Push + heartbeat | **Push + heartbeat** |
| OD-06 | NP intra-request failover | Required for zero msg failure? | **Yes (max 2 alternates)** |
| OD-07 | Latency SLO | &lt;20 ms vs &lt;200 ms | **Confirmed: &lt; 20 ms** |
| OD-08 | Planned drain signal | Ops API / CNPG hook / reactive only | **Support API + reactive** |
| OD-09 | UP gating | Single probe vs N probes + warm | **N probes + warm** |

---

## 16. Implementation Phases (After Confirmation)

> **No code until HLD + OD-01…OD-09 confirmed.**

| Phase | Deliverable |
| --- | --- |
| P0 | Freeze ODs + config schema |
| P1 | 3× HikariDataSource factory + boot with DB down |
| P2 | HealthyPoolSelector RR + quarantine |
| P3 | Worker path + PS execute + failover |
| P4 | Monitoring + Management + SLP/alarm SPI |
| P5 | Metrics + equal-LB tests |
| P6 | Restart chaos: drain, kill pod, verify zero loss when ≥1 pool UP; soak 5k qps |

---

## 17. Test Strategy

| Test | Intent |
| --- | --- |
| Unit | RR fairness 3/2/1 pools; quarantine; failover bounds |
| Integration | 3 PG instances on fixed IPs; PS path |
| Performance | 5k qps; p99 vs SLO |
| Restart chaos | Abrupt stop one pod under load → no NP failure if others UP; share → 50/50 then 33/33/33 |
| Planned drain | drainPool → restart → gated UP; zero failures |
| Negative | All pools down → fail once; bad IP; auth fail |

---

## 18. Document History

| Version | Date | Notes |
| --- | --- | --- |
| 0.1 | 2026-07-13 | Initial HLD from original requirements |
| 0.2 | 2026-07-13 | Aligned to stakeholder design; added restart equal-LB + no message failure controls; see DESIGN_REVIEW.md |
| 0.3 | 2026-07-13 | Confirmed query response time SLO: **&lt; 20 ms** (OD-07 closed) |

---

## 19. Confirmation Checkpoint

Confirm before implementation:

1. Borrow/return + 3× Hikari (max 17) + RR on UP pools  
2. **OD-06** NP intra-request failover for no message failure  
3. Extended states: UP / DRAINING / DOWN / RECOVERING  
4. NP timeout alignment to confirmed **&lt; 20 ms** response-time SLO  
5. Multus static IP–only JDBC URLs  

---

## 20. Postgres Restart: Equal LB + No Message Failure

This section is normative for the review focus.

### 20.1 Problem in Stakeholder Design (As Written)

1. Pool stays RR-eligible until Management processes a failure event.  
2. In-flight and newly selected queries to the restarting pod **fail to SLP** (no NP retry).  
3. Until DOWN, success distribution is **not** equal (one third may error).  
4. After recovery, single successful `SELECT 1` can mark UP too early → timeouts and uneven effective load.

### 20.2 Target Behaviors

| Phase | Equal load | Message outcome |
| --- | --- | --- |
| Before restart (3 UP) | 33/33/33 | Success |
| Planned drain of pod X | 50/50 on others **before** PG stops | **No new failures** |
| Abrupt crash of pod X | Within one request: failover to next UP; RR excludes X | **Success if ≥1 other UP** |
| Steady degraded | 50/50 (or 100%) | Success on survivors |
| Gated recovery | Still 50/50 until UP | No premature share |
| Post UP | 33/33/33 | Success |

### 20.3 Controls (Mandatory for FR-10 / NFR-06)

1. **Atomic quarantine** on first infra error (data plane).  
2. **Intra-request failover** up to 2 alternate pools (NP).  
3. **Planned `drainPool`** for maintenance restarts.  
4. **RECOVERING gate**: N consecutive probe successes + minIdle warm before UP.  
5. **Short NP acquire/query timeouts** so failover beats the latency SLO.  
6. **Ops guardrail**: do not restart all three pods concurrently.

### 20.4 Sequence — Abrupt Replica Restart

```
Replica-2 TCP resets
  → Worker infra error on R2
  → quarantine(R2)                      // RR now Primary↔R1 only (equal 50/50)
  → failover same request to next UP
  → SLP receives success
  → Management marks R2 DOWN, alarm, broadcast
  → Monitor probes R2 every 5s
  → R2 healthy → RECOVERING → warm → UP
  → RR returns to 33/33/33
```

### 20.5 Sequence — Planned Primary Restart

```
Ops → drainPool(primary)
  → DRAINING (removed from RR; in-flight completes)
  → Restart Postgres primary
  → Monitor → RECOVERING → UP
  → Equal 33/33/33 restored
```

### 20.6 Explicit Non-Goals During Restart

- Guaranteeing success when **all** pools are down or draining.  
- Retrying after a **business** SQL failure.  
- Keeping a restarting pool in RR “to preserve 33% share” — availability beats naive share during failure.
