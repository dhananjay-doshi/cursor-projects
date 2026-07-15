# High-Level Design: Number Portability DB Connection Management (`npdb-cnf`)

| Field | Value |
| --- | --- |
| Document | High-Level Design (HLD) |
| Module | Postgres Connection Management for Number Portability (NP) queries |
| Target runtime | Java (CNF / Kubernetes) |
| Connection pool | HikariCP |
| Database | PostgreSQL (1 Primary + 2 Replica), Multus static IPs |
| Status | **Draft v0.11 — DB Handler vs Service OAM two-module model (§3.3, §5.7)** |
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
| Common DB connector used by DB Handler + Service OAM | Yes |
| Service OAM single DML, bulk (50M), audit timeout/pool guidance | Yes |
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
| FR-11 | Client process **initializes and keeps running** if all Postgres pods are down; Hikari retries connections in background (`initializationFailTimeout=0`) | **CONFIRMED** |
| FR-12 | When **zero** pools are UP: fail fast (no borrow); CRITICAL alarm; SLP broadcast; monitor-driven recovery only | **CONFIRMED** |
| FR-13 | On infra failure: **quarantine routing eligibility atomically before/without waiting for Management**; event queue used for alarms/SLP only |

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
┌────────────────────────────┐
│  DB Handler pod            │
│  5K SELECT/sec, &lt;20 ms     │
│  16 workers + mgmt/monitor │
│  Hikari NP ×3 (max 17)     │
└──────────────┬─────────────┘
               │ Multus static IPs
    ┌──────────┼──────────┐
    ▼          ▼          ▼
 PG Primary  Replica-1  Replica-2
    ▲          ▲          ▲
    └──────────┼──────────┘
               │ Multus static IPs (same)
┌──────────────┴─────────────┐
│  Service OAM pod           │
│  Single I/U/D + bulk/audit │
│  Hikari OAM ×3 (max 3)     │
│  longer timeouts + retry   │
└────────────────────────────┘
```

### 3.3 Modules: DB Handler vs Service OAM

Database access is split across **two processes** (two Deployments). They use a **common DB connector** library and the same Multus Postgres IPs, but **separate JVMs ⇒ separate Hikari pools**. Provisioning cannot steal connections from the 5K select path.

| Dimension | **DB Handler** | **Service OAM** |
| --- | --- | --- |
| Purpose | Real-time NP **SELECT** (subscriber / routing) | Subscriber **provisioning** &amp; maintenance |
| Workloads | ≥ **5,000** lookups/sec, **&lt; 20 ms** | (1) Single `INSERT`/`UPDATE`/`DELETE` (2) **Bulk** load (e.g. **50M** rows) (3) **DB audit** |
| Hikari per PG pod | **min=max=17**, acquire **~50 ms** | **min=max=3**, acquire **1–5 s** |
| Routing | RR across UP primary + replicas | **Writes → PRIMARY**; audit/heavy read may use replicas |
| Retry | No business retry; infra failover ≤2 pools | Up to **2** infra retries; writes stay on primary |
| Isolation | Dedicated pools | Dedicated pools — **required** so 50M bulk does not impact 5K selects |

Inside Service OAM, single-row DML vs bulk vs audit share OAM pools and use different **timeout profiles** (§5.6.3, §5.7) — not DB Handler’s pools.

**Note:** “One pool for NP + bulk” only matters if both ran in one JVM. With two modules as above, that anti-pattern is already avoided across processes.

---

## 4. Logical Architecture

> Primary diagram below is **DB Handler** (5K SELECT path). Service OAM uses the same connector patterns with smaller pools and write-oriented routing (§3.3, §5.7).

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

### 5.2 Bootstrap When All Postgres Pods Are Down (**Required**)

**Application requirement:** the client process must **initialize and keep running** even if **all** Postgres server pods are down. HikariCP must **retry connection establishment in the background**.

| Setting / behavior | Value | Effect |
| --- | --- | --- |
| `initializationFailTimeout` | **0** | Pool creation does **not** fail the process if DB is unreachable; startup continues |
| `minimumIdle` = `maximumPoolSize` | **17** | Housekeeper keeps trying to fill the pool in the background when servers return |
| Worker / RR policy | Only UP pools | No query-path blocking on empty/DOWN pools while DB is out |
| Monitoring thread | Every 5 s | Probes each pool; drives RECOVERING → UP when background connects succeed |
| Initial pool FSM | DOWN or UNKNOWN → not RR-eligible | Process up; NP returns `NPDB_ALL_POOLS_UNAVAILABLE` until ≥1 pool UP |

Bootstrap sequence:

1. Load config (Multus IPs, secrets, pool/timeouts, SQL).
2. Create three `HikariDataSource` instances with `initializationFailTimeout = 0`.
3. Do **not** abort if warm-up `getConnection()` fails; mark pools DOWN/UNKNOWN.
4. Start Monitoring, Management, Workers — process is **in service** for non-DB concerns; NP queries fail fast until DB available.
5. Hikari **background** connection attempts refill toward `minimumIdle`; monitor confirms health and Management sets eligibility UP.

```
All PG pods down at start
  → Hikari pools created (no throw)
  → eligibility mask = 0
  → workers: nextUpPool() == null → NPDB_ALL_POOLS_UNAVAILABLE
  → Hikari housekeeper retries TCP/auth in background
  → first successful connections + gated probes → markEligible → RR resumes
```

### 5.3 Pool Sizing (Stakeholder-Aligned)

| Parameter | Per-pool | Rationale |
| --- | --- | --- |
| `maximumPoolSize` | **17** | 16 workers can all fail over onto one surviving pool + 1 monitor |
| `minimumIdle` | **17** | Fully warm; keeps long-lived connections; drives background refill after outage |
| Client total max | **51** | 17 × 3 (correct the attached doc’s “48” if max=17) |

**DBA check required** against Postgres `max_connections` (and other clients).

Service OAM / bulk provisioning should use a **separate Hikari family** (e.g., min=max=3 per pod) with longer timeouts — see §5.6. Do not share the NP pool’s 50 ms acquire timeout with bulk DML.

### 5.4 Production HikariCP + JDBC Property Review (NP)

Goals for this review:

1. **Long-lived TCP connections** — avoid Hikari churn (frequent close/reopen)  
2. **Connection loss detection as soon as practical**  
3. **Minimum query response** (&lt; 20 ms SLO)  
4. **Boot / run with all DBs down** + background reconnect  

#### 5.4.1 HikariCP properties

| Property | Stakeholder | **Production NP recommendation** | Review |
| --- | --- | --- | --- |
| `maximumPoolSize` | 17 | **17** | Adopt — supports full worker failover to one pod |
| `minimumIdle` | 17 | **17** | Adopt — equal to max ⇒ no idle eviction; background refill after outage |
| `idleTimeout` | N/A (min=max) | **0** (or leave unused) | Surplus idle close never applies when min=max; set **0** explicitly |
| `maxLifetimeTime` | 1800000 (30 min) | **0** (infinite) | Stakeholder 30 min **forces periodic reconnect** — conflicts with “long TCP”. Prefer **0**. If a firewall/NAT idle limit exists on Multus path, set to **slightly below** that limit (e.g. 25–50 min), not aggressively low |
| `keepaliveTime` | 30000 | **15000–30000** (default **20000**) | Idle connection validation so dead sockets are replaced **without** killing healthy long-lived conns. 15–20 s balances faster loss detection vs probe load; app monitor is already 5 s at pool level |
| `initializationFailTimeout` | 0 | **0** | **Required** — process starts with all PG pods down |
| `connectionTimeout` | 1000 | **50** ms (tune 20–50) | Stakeholder **1000 ms violates &lt;20 ms** if a worker ever waits on acquire. With UP-only RR this is mostly a safety net; keep **fail-fast** |
| `validationTimeout` | 250 | **200** ms | Bound validation during keepalive/`isValid`; must stay &lt; typical acquire budget when validation runs |
| `connectionTestQuery` | (default isValid) | **omit** (use JDBC4 `isValid`) | Prefer driver `isValid`; avoid extra SQL unless required by old drivers |
| `autoCommit` | — | **true** | NP lookups are read, single-statement |
| `readOnly` | — | **true** on replica pools; primary **false** or true if primary is read-only for NP | Safety + planner hints |
| `poolName` | — | `npdb-primary` / `npdb-r1` / `npdb-r2` | Metrics / JMX clarity |
| `registerMbeans` | true | **true** | Production troubleshooting |
| `leakDetectionThreshold` | — | **0** in steady prod (or 60000 in lab) | &gt;0 adds overhead; enable temporarily to catch borrow leaks |
| `allowPoolSuspension` | — | **false** | Not needed for NP |

#### 5.4.2 PostgreSQL JDBC URL / driver properties

**Units matter:** pgJDBC `connectTimeout` and `socketTimeout` are in **seconds**. JDBC `Statement.setQueryTimeout` is also **seconds**. Sub-20 ms control cannot use those APIs alone.

| Property | Stakeholder | **Production NP recommendation** | Review |
| --- | --- | --- | --- |
| Host | Multus static IP | **Multus static IP only** | Mandatory |
| `tcpKeepAlive` | true | **true** | OS-level dead peer detection on long-lived TCP — **required** |
| `tcpNoDelay` | — | **true** | **Add** — disable Nagle; lowers latency for small NP queries |
| `connectTimeout` | 1 (sec) | **1** (sec) | OK for **background** connect attempts; not on warm UP-path acquire |
| `socketTimeout` | 1 (sec) | **2** (sec) as **hard safety net** | 1 s is too coarse for &lt;20 ms but prevents multi-minute TCP blackhole hangs. Do **not** set to 0. Primary fail-fast = app deadline + cancel (below), not `socketTimeout` |
| `loginTimeout` | — | **2** (sec) | **Add** — bound auth handshake during background reconnect |
| `cancelSignalTimeout` | — | **1** (sec) | **Add** — so query cancel cannot hang |
| `ApplicationName` | — | `npdb-cnf` (or process name) | **Add** — Postgres `pg_stat_activity` clarity |
| `prepareThreshold` | 3 | **1** | Faster server-side prepared statements on hot NP path |
| `preparedStatementCacheQueries` | 20 | **20–32** | Adopt / slight headroom |
| `preparedStatementCacheSizeMiB` | 5 | **5** | Adopt |
| `reWriteBatchedInserts` | — | leave default | NP is lookup, not batch write |
| SSL | (env specific) | per security policy (`sslmode`) | Do not disable if platform requires TLS |

Example JDBC URL:

```text
jdbc:postgresql://<MULTUS_IP>:5432/m7np_db
  ?tcpKeepAlive=true
  &tcpNoDelay=true
  &connectTimeout=1
  &socketTimeout=2
  &loginTimeout=2
  &cancelSignalTimeout=1
  &ApplicationName=npdb-cnf
  &prepareThreshold=1
  &preparedStatementCacheQueries=32
  &preparedStatementCacheSizeMiB=5
```

#### 5.4.3 Application-level timeouts (required for &lt;20 ms)

Because JDBC `socketTimeout` / `setQueryTimeout` are **second-granularity**, the NP connector must enforce a **millisecond** budget:

| Control | Recommendation | Role |
| --- | --- | --- |
| Hikari `connectionTimeout` | **50 ms** | Fail acquire fast → quarantine/failover |
| Per-query deadline + `Statement.cancel()` | **15–20 ms** watchdog | Enforce &lt;20 ms; triggers failover path on hang |
| Optional `Connection.setNetworkTimeout(executor, ms)` | Use cautiously (e.g. **100–200 ms** ceiling) | JDBC ms API; marks connection closed on expiry — keep **above** normal query time so healthy slow-ish queries are not killed; tighter than `socketTimeout` seconds |

**Do not** use stakeholder **2 s** statement timeout on the NP path.

#### 5.4.4 Mapping goals → settings

| Goal | How settings achieve it |
| --- | --- |
| Long TCP, little churn | `minIdle=maxPool=17`, `idleTimeout=0`, `maxLifetimeTime=0`, return connections to pool after each query (borrow/return does **not** close TCP) |
| Fast loss detection | `tcpKeepAlive=true`, Hikari `keepaliveTime≈20s`, app monitor **5 s**, worker infra errors → **atomic quarantine**, ms query deadline/cancel |
| Minimum query response | Warm pool, `tcpNoDelay`, `prepareThreshold=1`, PS cache, short `connectionTimeout`, no DOWN-pool acquires, Multus static IP |
| Run while all DB down | `initializationFailTimeout=0`, UP-only RR, background housekeeper refill |

#### 5.4.5 Platform / OS complements (ops)

| Item | Guidance |
| --- | --- |
| Linux TCP keepalive sysctls | Optionally tune `tcp_keepalive_time/intvl/probes` on client pod for faster dead-peer detection than OS defaults (often 2 hours) — coordinate with platform |
| Firewall idle timeout | If present, either exempt Multus NP flows or set Hikari `maxLifetimeTime` just below that value |
| Postgres `max_connections` | ≥ 51 × app replicas (+ admin/monitor headroom) |
| CNPG / PG `idle_session_timeout` | Must be **0/disabled** or higher than app expectations so server does not close long-lived pooled conns |

### 5.5 PreparedStatement Strategy

Borrow connection → driver-cached / server-side prepared statement (`prepareThreshold=1`) → `close()` returns connection to Hikari (**TCP session kept** in pool).

No application-owned PreparedStatement cache across pool returns.

### 5.6 Different Timeouts: DB Handler (SELECT) vs Service OAM (Provisioning)

**DB Handler** and **Service OAM** are separate processes (§3.3) ⇒ separate Hikari instances. Timeout families below map to those modules—not to two logical pools inside one NP JVM unless OAM is co-located (not recommended).

HikariCP attaches **`connectionTimeout` (and the JDBC URL) to the pool**, not to each borrow — so within **one** process you still cannot have two Hikari acquire timeouts on one `HikariDataSource` without two pools.

#### 5.6.1 Recommended approach: **two pool families** to the same Multus IPs

```
                    ┌─────────────────────────┐
                    │ HealthyPoolSelector (NP)│  RR + quarantine + &lt;20 ms path
                    └───────────┬─────────────┘
          ┌─────────────────────┼─────────────────────┐
          ▼                     ▼                     ▼
     NP Hikari×3           NP Hikari×3           NP Hikari×3
     (max 17, TO=50ms)     …                     …
          │                     │                     │
          └──────────┬──────────┴──────────┬──────────┘
                     │   Multus static IPs │
          ┌──────────┴──────────┬──────────┴──────────┐
          ▼                     ▼                     ▼
     BULK Hikari×3         BULK Hikari×3         BULK Hikari×3
     (max 3, TO=1–5s)      …                     …
          ▲                     ▲                     ▲
          └─────────────────────┼─────────────────────┘
                    ┌───────────┴─────────────┐
                    │ Bulk/OAM DbConnector    │  longer deadlines, OAM retry
                    └─────────────────────────┘
```

| Family | Workload | Pool size (per PG pod) | Acquire `connectionTimeout` | Query / network budget | RR / writes |
| --- | --- | --- | --- | --- | --- |
| **NP** | Subscriber lookup (workers) | **17 / 17** | **50 ms** | **~15–20 ms** deadline + cancel | RR reads across UP primary+replicas |
| **BULK / OAM** | Add / modify / delete (and other provisioning) | **3 / 3** (stakeholder OAM note) | **1000–5000 ms** | **seconds** (e.g. `setQueryTimeout` 5–30 s, batch-aware) | Prefer **PRIMARY** for writes; reads optional |

Shared across both families:

- Same Multus static IPs / DB name / credentials (or bulk-specific DB role with DML rights)
- `initializationFailTimeout = 0`, long-lived TCP (`maxLifetimeTime=0`, `idleTimeout=0`, `tcpKeepAlive`, `tcpNoDelay`)
- Independent UP/DOWN quarantine optional but recommended (bulk write outage ≠ necessarily NP read outage on replicas)

**Why this is preferred**

| Concern | Separate pools | Single shared pool |
| --- | --- | --- |
| Different Hikari `connectionTimeout` | **Yes** — native | **No** — one value for all borrows |
| Bulk holding connections starves NP | Isolated capacity | High risk under 5k qps |
| JDBC URL `socketTimeout` | Can differ per family | One URL per pool — coarse for both |
| NP &lt;20 ms SLO under bulk load | Protected | Easily broken |
| Postgres `max_connections` | 17×3 + 3×3 = **60**/client process | Lower total, but coupled risk |

#### 5.6.2 When a **single pool** is used for NP **and** bulk

If product constraint is “one HikariCP pool (per Postgres pod) for both normal lookups and bulk add/modify/delete,” accept these facts:

| Fact | Implication |
| --- | --- |
| One `connectionTimeout` | Acquire wait is **identical** for NP and bulk (must pick a compromise, e.g. 200–500 ms) |
| One JDBC URL / `socketTimeout` | Hard socket safety net is **identical** (pick bulk-oriented, e.g. 60 s, or NP-oriented 2 s — not both) |
| Connections are fungible | A bulk op can occupy a connection that an NP worker wanted → risk to **&lt;20 ms** and 5k qps |
| In-query timeouts can still differ | After `getConnection()`, apply **NP vs BULK profile** before execute (§5.6.3), then **reset** on return |

```
                    ┌──────────────────────┐
                    │ Single Hikari pool   │  per Multus IP
                    │ connectionTimeout=X  │  ← one value for everyone
                    │ maxPoolSize=17       │
                    └──────────┬───────────┘
               borrow│         │borrow
         ┌───────────┘         └───────────┐
         ▼                                 ▼
  NP worker                          Bulk/OAM thread
  apply(NP profile)                  apply(BULK profile)
  execute lookup                     execute DML/batch
  reset + close()                    reset + close()
```

##### Mandatory controls for single-pool mode

1. **Per-borrow timeout profile** (before query) — only way to differentiate NP vs bulk once connected.  
2. **Reset profile on return** — or the next NP borrow inherits bulk `setNetworkTimeout`.  
3. **Cap bulk concurrency** — e.g. semaphore `maxInFlightBulk = 1 or 2` so bulk cannot take most of the 17 connections.  
4. **Chunk bulk work** — short transactions; do not hold a borrowed connection across large file parsing pauses.  
5. **Prefer PRIMARY for bulk writes**; NP may still RR including replicas **on the same pools** (writes and reads share capacity).  
6. **Compromise acquire timeout** — e.g. **300 ms**: slower than ideal NP fail-fast (50 ms), faster than comfortable bulk (5 s). Document SLO impact.  
7. **Metrics** — track NP acquire wait, bulk in-flight, NP p99 separately; alert if bulk correlates with NP latency.

##### What single-pool **cannot** do

- Different Hikari `connectionTimeout` for NP vs bulk  
- Guarantee NP &lt;20 ms while bulk saturates the pool  
- Isolate bulk connection failures from NP capacity on that pod  

##### Single-pool timeout cheat-sheet

| Knob | Single-pool value | Per-op override |
| --- | --- | --- |
| Hikari `connectionTimeout` | **300 ms** compromise (tune) | None |
| JDBC `socketTimeout` | **60 s** (protect bulk) or **2 s** (protect NP hang) — choose explicitly | None |
| `setNetworkTimeout` | — | NP **200 ms** / Bulk **60_000 ms** before query |
| `setQueryTimeout` | — | NP **0** + 20 ms watchdog / Bulk **30 s** before query |
| Bulk in-flight | — | **≤ 2** (hard) |

**HLD preference remains separate pool families (§5.6.1).** Single-pool is a constrained mode with higher risk to the NP latency SLO.

#### 5.6.2b Alternative label: shared-pool + overlay

Same as §5.6.2: one shared pool + per-borrow timeout overlay + bulk concurrency cap.

---

#### 5.6.3 How to update the timeout profile **before** the query

Timeouts that can change **per operation** are applied on the **borrowed `Connection` / `Statement`**, after `getConnection()` and **before** `execute*()`. Hikari pool settings are fixed at pool creation and are **not** updated per query.

##### Order of operations (normative)

```
1. select pool (NP or BULK family)
2. Connection c = dataSource.getConnection()     // Hikari acquire timeout already applies
3. applyTimeoutProfile(c, profile)               // ← update profile HERE
4. PreparedStatement ps = c.prepareStatement(...)
5. applyStatementTimeout(ps, profile)            // statement-level where needed
6. startQueryDeadlineWatchdog(ps, profile)       // NP: ms cancel; bulk: optional
7. ps.executeQuery() / executeUpdate()
8. consume results
9. finally:
     stopWatchdog()
     resetTimeoutProfile(c)                      // mandatory before return
     ps.close()
     c.close()                                   // return to Hikari
```

##### What to set where

| Knob | When to set | NP | Bulk DML |
| --- | --- | --- | --- |
| Hikari `connectionTimeout` | Pool create only | 50 ms (NP pools) | 1–5 s (BULK pools) |
| JDBC URL `socketTimeout` | Pool/URL create only | 2 s safety net | 60 s safety net |
| `Connection.setNetworkTimeout(executor, ms)` | **Before query** (step 3) | 100–200 ms ceiling | 30_000–120_000 ms |
| `Statement.setQueryTimeout(seconds)` | **Before execute** (step 5) | Prefer **unset/0** + ms watchdog | **5–30** seconds |
| App cancel watchdog | **Before execute** (step 6) | **15–20 ms** → `ps.cancel()` | Optional (e.g. batch wall clock) |
| `SET statement_timeout` (PG GUC) | Optional in step 3 | Usually skip | Optional mirror of bulk budget |

##### Suggested helper API (design)

```java
public final class TimeoutProfile {
  public enum Kind { NP_LOOKUP, BULK_DML, OAM_READ }

  public final Kind kind;
  public final int networkTimeoutMs;   // Connection.setNetworkTimeout
  public final int queryTimeoutSec;    // Statement.setQueryTimeout; 0 = leave unset
  public final int deadlineMs;         // app watchdog; 0 = disabled

  public static final TimeoutProfile NP =
      new TimeoutProfile(Kind.NP_LOOKUP, 200, 0, 20);
  public static final TimeoutProfile BULK =
      new TimeoutProfile(Kind.BULK_DML, 60_000, 30, 0);
}

public final class TimeoutProfileApplier {
  private final Executor timeoutExecutor; // dedicated, not worker threads

  public void apply(Connection c, TimeoutProfile p) throws SQLException {
    // Update connection-level profile BEFORE creating/executing the statement
    c.setNetworkTimeout(timeoutExecutor, p.networkTimeoutMs);
  }

  public void apply(Statement s, TimeoutProfile p) throws SQLException {
    if (p.queryTimeoutSec > 0) {
      s.setQueryTimeout(p.queryTimeoutSec);
    } else {
      s.setQueryTimeout(0); // disabled; NP uses watchdog instead
    }
  }

  public void reset(Connection c) throws SQLException {
    // Prevent next borrower (especially NP) from inheriting bulk timeouts
    c.setNetworkTimeout(timeoutExecutor, 0);
  }
}
```

##### Call site pattern (before query)

**NP lookup (separate NP pool — recommended):**

```java
TimeoutProfile profile = TimeoutProfile.NP;
try (Connection c = npDataSource.getConnection()) {
  timeoutApplier.apply(c, profile);                 // before query
  try (PreparedStatement ps = c.prepareStatement(NP_SQL)) {
    timeoutApplier.apply(ps, profile);
    ps.setString(1, subscriberNumber);
    try (QueryDeadline d = QueryDeadline.start(ps, profile.deadlineMs)) {
      try (ResultSet rs = ps.executeQuery()) {
        return map(rs);
      }
    }
  }
} finally {
  // if not using try-with-resources for Connection alone, always:
  // timeoutApplier.reset(c);
}
```

With try-with-resources, prefer a small wrapper so **reset always runs**:

```java
try (ProfiledConnection pc = ProfiledConnection.borrow(npDataSource, TimeoutProfile.NP, timeoutApplier)) {
  // apply() already done inside borrow()
  try (PreparedStatement ps = pc.connection().prepareStatement(NP_SQL)) {
    timeoutApplier.apply(ps, TimeoutProfile.NP);
    ...
  }
} // close() → reset(connection) then connection.close()
```

**Bulk add/modify/delete (BULK pool):**

```java
try (ProfiledConnection pc = ProfiledConnection.borrow(bulkPrimaryDs, TimeoutProfile.BULK, timeoutApplier)) {
  try (PreparedStatement ps = pc.connection().prepareStatement(UPSERT_SQL)) {
    timeoutApplier.apply(ps, TimeoutProfile.BULK); // setQueryTimeout(30) before execute
    // bind … batch …
    ps.executeBatch();  // or executeUpdate()
  }
} // reset + return to pool
```

##### `QueryDeadline` (NP ms budget)

JDBC `setQueryTimeout` cannot express &lt;20 ms. Before `executeQuery()`:

```text
deadline = now + 20ms
schedule on timeoutExecutor:
  if still running → PreparedStatement.cancel()
executeQuery()
cancel scheduled task on completion
```

On cancel/`SQLException`: treat as infra failure if connection is broken; quarantine + failover per §6.3.

##### Reset rules (critical)

| Rule | Why |
| --- | --- |
| Always `setNetworkTimeout(executor, 0)` before return | Next NP borrow must not inherit 60 s bulk network timeout |
| Do not rely on pool to clear statement timeouts | New `PreparedStatement` each time is OK; connection-level state **persists** |
| Reset in `finally` / wrapper `close()` | Exceptions must not skip reset |
| Never call `setNetworkTimeout` from random app threads without a stable `Executor` | JDBC requires an executor for async abort |

##### What not to do

| Anti-pattern | Problem |
| --- | --- |
| Change Hikari `connectionTimeout` per query | Not supported; pool-wide only |
| Put bulk `socketTimeout` on the NP pool URL | All NP connections inherit seconds-level hang |
| Apply bulk profile and return connection without reset | Contaminates NP path |
| Use worker thread as `setNetworkTimeout` executor | Can deadlock / starve workers |
| Set NP `Statement.setQueryTimeout(1)` expecting 20 ms | Unit is **seconds** — minimum practical 1 s |

##### With separate NP vs BULK pools (default)

Still **apply a profile before every query**:

- NP pool: short network ceiling + 20 ms watchdog  
- BULK pool: long network + `setQueryTimeout` in seconds  

Pool family chooses **acquire** behavior; profile chooses **in-query** behavior. Both layers are required.

---

#### 5.6.4 Failover / retry differences

| | NP | Bulk / OAM |
| --- | --- | --- |
| Infra failure | Atomic quarantine + up to 2 alternate **read** pools | OAM: up to 2 alternate attempts; **writes should stick to PRIMARY** (or documented failover role) |
| Business SQL error | No retry | Provisioning policy (idempotent retry) |
| All pools down | `NPDB_ALL_POOLS_UNAVAILABLE` | Same / OAM-specific error; process stays up |

#### 5.6.5 Decision

| Option | HLD default |
| --- | --- |
| **A. Separate NP + BULK Hikari families** (same Multus IPs) | **Recommended / default** |
| B. Single shared pool + per-borrow profiles + bulk concurrency cap | Fallback only |

Open decision **OD-13**: confirm separate OAM pools in Service OAM process (max 3) — **default yes**; DB Handler never shares pools with OAM.

### 5.7 Service OAM Workloads on OAM Pools (Single vs Bulk vs Audit)

Service OAM uses **its own** Hikari pools (not DB Handler’s). Within OAM, apply a timeout profile **before each query** (§5.6.3).

| Workload | SQL pattern | Pool target | Timeout profile (illustrative) | Notes |
| --- | --- | --- | --- | --- |
| **Single I/U/D** | One row insert/update/delete | **PRIMARY** | network 5–10 s; `setQueryTimeout` 5–10 s | OAM retry ≤2 on infra error; keep transactions short |
| **Bulk provision** (e.g. 50M subscribers) | Batched multi-row DML | **PRIMARY** | network 60–120 s; `setQueryTimeout` 30–60 s **per batch** | Never one transaction for 50M rows |
| **DB audit** | Long read / compare queries | PRIMARY or replica | network 60–300 s; query timeout per statement | Prefer replica to spare primary write capacity |

#### Bulk 50M — connection usage rules

1. **Chunk** into batches (e.g. 1K–10K rows / `executeBatch`); commit per chunk.  
2. **Borrow → apply BULK profile → execute batch → reset → close** per chunk (or small group of chunks), not for the entire 50M job.  
3. **Limit in-flight bulk connections** to ≤ pool size (typically **1–2** concurrent bulk workers) so single I/U/D and audit still get a connection from max=3.  
4. **Checkpoint** progress (last key / offset) for restart after failure.  
5. **Do not** lower DB Handler pool sizes during bulk — separate process already isolates selects.  
6. Watch Postgres: WAL, CPU, replication lag; throttle bulk if replicas lag impacts DB Handler reads (ops policy).  
7. Optional: schedule huge loads in maintenance windows; still keep DB Handler up with `initializationFailTimeout=0` semantics unchanged.

#### OAM pool sizing reminder

| Setting | OAM value | Why |
| --- | --- | --- |
| `maximumPoolSize` / `minimumIdle` | **3 / 3** | 1 interactive/single DML + 1 bulk + 1 monitor (stakeholder) |
| `connectionTimeout` | **1000–5000 ms** | Provisioning can wait; not on &lt;20 ms path |
| `initializationFailTimeout` | **0** | OAM process also starts if Postgres is down |
| `maxLifetimeTime` | **0** | Long-lived TCP, same rationale as NP |

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

### 6.4 Last Remaining UP Pool Restarts / Goes Down (**CONFIRMED**)

Worker selection rule (**confirmed**): **each worker picks the next pool that is already UP** (round-robin among UP only).

When the system has already degraded to a **single UP pool**, and that last pool also restarts or fails:

```
UP count: 3 → 2 → 1 → 0
                         ▲
                         └── last pool infra failure / restart
```

#### Confirmed behavior (normative)

| Step | Action | Rationale |
| --- | --- | --- |
| 1 | Worker hits infra error on the last UP pool | Same detection as any other pool |
| 2 | **Atomically quarantine** that pool (UP → DRAINING/DOWN in selector) | Do **not** keep a known-dead pool in RR “because it is the last one” — that violates &lt;20 ms via timeouts |
| 3 | `nextUpPool()` returns **null** (zero UP pools) | No alternate target for failover |
| 4 | **Do not** call `getConnection()` on DOWN/DRAINING pools | Prevents connectionTimeout storms and SLO breach |
| 5 | Return **immediate** failure to SLP with distinct cause `NPDB_ALL_POOLS_UNAVAILABLE` | Fail-fast; caller/SLP applies existing error handling |
| 6 | Enqueue event → Management raises **CRITICAL** alarm + broadcasts “all NP DB DOWN” | Upstream can stop/shed NP traffic; ops notified |
| 7 | Monitoring continues probing **all three** pools every 5 s | Sole recovery path while workers stay fail-fast |
| 8 | First pool to pass gated recovery → UP; RR resumes (100% on that one, then equalize as others return) | Automatic restoration |

```
Worker on last UP pool
  → infra failure
  → quarantine(lastPool)          // UP count becomes 0
  → nextUpPool() == null
  → return NPDB_ALL_POOLS_UNAVAILABLE to SLP   // no Hikari borrow
  → Management: CRITICAL alarm + SLP broadcast
  → Monitor: probe P/R1/R2 until RECOVERING→UP
```

#### What not to do

| Anti-pattern | Why reject |
| --- | --- |
| Keep last pool UP after infra failure | Workers block on dead connections; blows &lt;20 ms SLO |
| Blindly try DOWN pools from workers | Same timeout storm; defeats healthy-only RR |
| Sleep/retry in worker waiting for recovery | Violates latency SLO; blocks worker capacity |
| Claim “no message failure” when UP count = 0 | Impossible without a live Postgres endpoint |

#### Guarantees (honest)

| Situation | NP message outcome |
| --- | --- |
| ≥1 other pool still UP | Failover → **success** (no message failure) |
| Last / only UP pool also down | **Fail fast** to SLP — unavoidable until monitor restores ≥1 pool |
| All three down at process start | Workers fail fast; Hikari background reconnect + monitor bring pools UP later |

#### SLP / application coordination

When Management broadcasts **all pools DOWN**:

1. SLP should treat NP DB as unavailable (alarm already raised).  
2. Optional: SLP sheds or rejects new NP queries locally to protect the node (policy outside this module).  
3. When any pool returns to UP, broadcast clears CRITICAL / sets degraded-or-clear; workers resume RR automatically.

Service OAM may still attempt its max-2 alternate-pool policy, but with zero UP pools it must likewise fail fast after selector returns null (no point burning OAM threads on DOWN pools).

### 6.5 Closing the Worker→Management Event Lag Window

#### Problem

```
t0  Worker-A detects connection loss on Pool-X
t1  Worker-A enqueues DbEvent(Pool-X DOWN)     ← event posted
t2  … Management thread has NOT yet dequeued/processed …
t3  Worker-B / Worker-C still see Pool-X as UP → RR selects X → MORE message loss
t4  Management processes event → marks Pool-X DOWN
```

If routing eligibility waits for Management, **every query that selects Pool-X in [t1, t4]** can fail. Under 5k qps that window can mean hundreds of lost messages even if Management is only a few milliseconds late.

#### Principle: split data-plane routing from control-plane authority

| View | Owner | Purpose | Updated when |
| --- | --- | --- | --- |
| **Routing eligibility mask** | Shared atomic structure read by **all workers** | Who may be selected by RR / failover | **Immediately** on detect (before or without waiting for Management) |
| **Authoritative pool FSM** | Management thread only | Alarms, SLP broadcast, confirmed UP/DOWN/RECOVERING | When event is processed |

Management remains the only writer of alarms/SLP status. Workers are allowed to **clear eligibility bits** (quarantine) instantly. Only Management (or monitor recovery path via Management) **sets eligibility back to UP**.

#### Required order of operations in the detecting worker

```
on infra failure for Pool-X:
  1. CAS / atomic quarantine(Pool-X)     // eligibility bit OFF — visible to all workers NOW
  2. enqueue DbEvent(Pool-X, DRAINING/DOWN)  // control plane (may lag)
  3. failover: nextUpPool() among still-eligible pools
  4. return success or NPDB_ALL_POOLS_UNAVAILABLE
```

**Never** enqueue first and quarantine only after Management acknowledges.

#### Suggested shared structure (illustrative)

```text
class HealthyPoolSelector {
  // bit0=Primary, bit1=R1, bit2=R2; 1 = eligible for RR
  final AtomicInteger eligibilityMask;   // workers: clear bits only
  final AtomicInteger rrCursor;

  PoolId nextUpPool() {
    // read mask once; RR among bits that are set; skip ineligible
  }

  boolean quarantine(PoolId id) {
    // CAS clear bit; return true if this caller transitioned 1→0 (first detector)
  }

  // called ONLY from Management after gated recovery
  void markEligible(PoolId id) { /* CAS set bit */ }
}
```

Illustrative Java shape:

```java
// First detector wins; others are no-ops for the bit clear
int bit = 1 << poolOrdinal;
int prev = eligibilityMask.getAndUpdate(m -> m & ~bit);
boolean firstToQuarantine = (prev & bit) != 0;
if (firstToQuarantine) {
  eventQueue.offer(DbEvent.down(poolId, cause)); // coalesce-friendly
}
```

#### How this minimizes message loss

| Mechanism | Effect on [t1, t4] window |
| --- | --- |
| **1. Atomic quarantine before enqueue** | Other workers stop selecting Pool-X on the **next** `nextUpPool()` call — typically microseconds, not Management latency |
| **2. Intra-request failover (OD-06)** | The detecting worker’s **own** message is saved on another UP pool |
| **3. First-detector-only event** | Avoids flooding the queue; Management still sees one DOWN |
| **4. Event coalescing in queue** | Multiple late detects for same pool collapse to one FSM transition |
| **5. Short NP timeouts** | Any in-flight borrow already on Pool-X fails fast and also quarantines (idempotent) + failovers |
| **6. Management lag becomes harmless for routing** | Lag only delays alarm/SLP broadcast, not RR exclusion |

```
Without atomic quarantine:
  detect → enqueue ~~~~~~~~~~ Management ──► DOWN
              └── other workers still hit Pool-X ──► message loss

With atomic quarantine:
  detect → quarantine (mask) ──► all workers skip Pool-X immediately
        → enqueue ~~~~~~~~~~ Management ──► alarm/SLP only
        → failover same request ──► success if ≥1 other UP
```

#### What Management still does (after the lag)

1. Confirm FSM: DRAINING/DOWN  
2. Raise alarm / clear as appropriate  
3. Broadcast to SLP (may be slightly delayed — acceptable; routing already correct)  
4. **Never** auto-clear quarantine from a worker path  
5. On monitor recovery: RECOVERING → gated UP → `markEligible(pool)` so RR re-admits the pool

#### Residual unavoidable loss

Atomic quarantine does **not** save:

- Queries **already executing** on Pool-X when it dies (in-flight) — those fail; failover applies only if the worker catches the error and alternates  
- Queries that selected Pool-X **in the same nanoseconds** before the CAS becomes visible — vanishingly small vs queue lag  
- The case when **no other pool is UP** (§6.4)

#### Design rule (normative)

> **Routing decisions must not wait on the Management event queue.**  
> The queue is for **control-plane side effects** (FSM confirmation, alarms, SLP broadcast), not for making a dead pool unselectable.

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

**Important:** Enqueueing an event does **not** make the pool unselectable. That is done by **atomic quarantine on the routing mask** (§6.5) so Management processing lag cannot cause additional RR selections of the dead pool.

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
    idleTimeoutMs: 0                 # long-lived; min==max
    connectionTimeoutMs: 50          # NP fail-fast acquire
    validationTimeoutMs: 200
    keepaliveTimeMs: 20000           # idle dead-socket detection without churn
    maxLifetimeTimeMs: 0              # 0 = no periodic recycle (long TCP)
    initializationFailTimeoutMs: 0   # boot with all DB down
    registerMbeans: true
  bulkPool:                            # separate family (§5.6) — same Multus IPs
    maximumPoolSize: 3
    minimumIdle: 3
    idleTimeoutMs: 0
    connectionTimeoutMs: 5000        # bulk acquire can wait
    validationTimeoutMs: 1000
    keepaliveTimeMs: 20000
    maxLifetimeTimeMs: 0
    initializationFailTimeoutMs: 0
    preferPrimaryForWrites: true
  jdbc:
    tcpKeepAlive: true
    tcpNoDelay: true
    connectTimeoutSec: 1             # background connect only
    socketTimeoutSec: 2              # NP family hard safety net (seconds!)
    loginTimeoutSec: 2
    cancelSignalTimeoutSec: 1
    applicationName: npdb-cnf
    prepareThreshold: 1
    preparedStatementCacheQueries: 32
    preparedStatementCacheSizeMiB: 5
  jdbcBulk:                            # bulk family may use longer socket safety net
    applicationName: npdb-bulk
    socketTimeoutSec: 60
    prepareThreshold: 1
  query:
    deadlineMs: 20                   # NP app watchdog + Statement.cancel
    networkTimeoutCeilingMs: 200     # optional Connection.setNetworkTimeout (NP)
  bulkQuery:
    queryTimeoutSec: 30
    networkTimeoutMs: 60000
    maxInFlight: 2                   # if ever sharing NP pool (fallback)
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

```
┌─────────────────────┐   ┌─────────────────────┐
│ DB Handler Deploy   │   │ Service OAM Deploy  │
│ Multus NAD + secrets│   │ Multus NAD + secrets│
│ NP Hikari ×3        │   │ OAM Hikari ×3       │
└──────────┬──────────┘   └──────────┬──────────┘
           │                         │
           └────────────┬────────────┘
                        ▼
              PG Primary + 2 Replicas
                 (Multus static IPs)
```

- NetworkPolicy + `pg_hba` allow both client Multus CIDRs to Postgres.  
- Postgres `max_connections` must cover **(17×3 × DB Handler replicas) + (3×3 × OAM replicas) + platform**.  
- Guardrail: avoid restarting all three PG pods concurrently.  
- Bulk 50M jobs run only in **Service OAM**; never in DB Handler workers.

---

## 14. Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Restart failure window while pool still UP | Atomic routing quarantine **before** event enqueue + intra-request failover (§6.5, §20) |
| Management event lag after worker detect | Eligibility mask cleared in detecting worker; queue lag only delays alarm/SLP (§6.5) |
| Early UP after restart | RECOVERING + N probes + minIdle warm |
| Bulk holds NP pool connections | Separate BULK pool family (§5.6); else cap bulk in-flight |
| Mixed NP/bulk timeouts on one pool | Hikari `connectionTimeout` is per-pool — use two families or accept compromise |
| 51 client connections | DBA approval of `max_connections` |
| Replica lag | Document consistency; optional primary-preferred later |
| Event queue burst | Coalesce; never drop first DOWN; heartbeat |
| Rolling restart of all 3 | Alarm if UP count &lt; 2; ops runbook |

---

## 15. Open Decisions (Confirm Before Coding)

| # | Topic | Options | HLD Default |
| --- | --- | --- | --- |
| OD-01 | Pool size | 17 per pool vs smaller | **17 / 17** (stakeholder) |
| OD-11 | `maxLifetimeTime` | 0 (long TCP) vs 30 min recycle | **0** unless firewall requires otherwise |
| OD-12 | `keepaliveTime` | 15s / 20s / 30s | **20000 ms** |
| OD-13 | OAM vs Handler pools | Separate processes/pools vs co-located shared | **Separate processes + OAM pools max 3 (§3.3, §5.7)** |
| OD-14 | Bulk batch size / concurrency | Tune in soak | **Start 1K–10K rows/batch, ≤2 bulk in-flight** |
| OD-02 | Include Primary in RR | Yes / replicas-only | **Yes — all 3** |
| OD-03 | Connection model | Borrow/return vs leased slots | **Borrow/return** |
| OD-04 | Metrics backend | Micrometer / JMX / legacy | **JMX + Micrometer if available** |
| OD-05 | Status API | Push + heartbeat | **Push + heartbeat** |
| OD-06 | NP intra-request failover | Required for zero msg failure? | **Yes (max 2 alternates)** |
| OD-07 | Latency SLO | &lt;20 ms vs &lt;200 ms | **Confirmed: &lt; 20 ms** |
| OD-08 | Planned drain signal | Ops API / CNPG hook / reactive only | **Support API + reactive** |
| OD-09 | UP gating | Single probe vs N probes + warm | **N probes + warm** |
| OD-10 | Last UP pool also down | Keep in RR vs fail-fast | **Confirmed: fail-fast + CRITICAL + SLP broadcast (§6.4)** |

---

## 16. Implementation Phases (After Confirmation)

> **No code until HLD + open decisions confirmed (see §15 / §19).**

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
| 0.4 | 2026-07-13 | Last remaining UP pool down: fail-fast policy (§6.4 / §20.7) |
| 0.5 | 2026-07-13 | **Confirmed** last-pool-down fail-fast + CRITICAL alarm/SLP broadcast (OD-10 / FR-12) |
| 0.6 | 2026-07-13 | §6.5: minimize loss in worker-detect → management-process lag via atomic routing quarantine |
| 0.7 | 2026-07-13 | FR-11 boot with all DB down; production Hikari/JDBC review for long TCP, fast loss detect, &lt;20 ms |
| 0.8 | 2026-07-13 | §5.6: different timeouts for bulk add/modify/delete vs NP via separate Hikari pool families |
| 0.9 | 2026-07-13 | §5.6.3: concrete apply/reset timeout profile before query pattern |
| 0.10 | 2026-07-13 | §5.6.2: expanded single shared-pool behavior for NP + bulk |
| 0.11 | 2026-07-13 | §3.3 / §5.7: DB Handler (5K SELECT) vs Service OAM (single IUD, 50M bulk, audit) |

---

## 19. Confirmation Checkpoint

Confirm before implementation:

1. Borrow/return + 3× Hikari (max 17) + RR on UP pools — **worker picks next UP pool: confirmed**  
2. **OD-06** NP intra-request failover for no message failure (when ≥1 other pool UP)  
3. Extended states: UP / DRAINING / DOWN / RECOVERING  
4. NP timeout alignment to confirmed **&lt; 20 ms** response-time SLO — **confirmed**  
5. Multus static IP–only JDBC URLs  
7. Client init with all Postgres pods down + Hikari background reconnect — **CONFIRMED** (FR-11 / §5.2)  
8. Production property set in §5.4 (esp. `maxLifetimeTime=0`, `connectionTimeout=50ms`, `tcpNoDelay`, ms query deadline) — review/approve  
9. DB Handler vs Service OAM as **separate processes/pools** (§3.3) — confirm  
10. OAM bulk 50M chunking + timeout profiles (§5.7 / OD-14) — confirm  

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
| Steady degraded (1 UP) | 100% on survivor | Success on survivor |
| **Last UP pool also down** | RR empty | **Fail fast** `NPDB_ALL_POOLS_UNAVAILABLE`; CRITICAL alarm (§6.4) |
| Gated recovery | Still exclude until UP | No premature share |
| Post UP | 33/33/33 (or 100% if only one recovered) | Success |

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

### 20.7 Last Pool Down (Detail)

See **§6.4**. Summary: quarantine the last pool, return `NPDB_ALL_POOLS_UNAVAILABLE` immediately, CRITICAL alarm + SLP broadcast, recover only via monitoring — never force workers onto DOWN pools.
