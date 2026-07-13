# Design Review: Stakeholder CNF Connection Management Design

| Field | Value |
| --- | --- |
| Document | Architectural review of attached stakeholder design vs original requirements |
| Reviewed artifact | PostgreSQL Connection Management Design for NPDB CNF Deployment |
| Baseline requirements | Original NPDB CNF brief (VNF→CNF, HikariCP, Multus static IPs, 5k qps) |
| Focus | Equal load distribution during Postgres restart **with no message failure** |
| Status | Review complete — design gaps and recommended controls captured |
| Related | [`HLD.md`](./HLD.md) updated to incorporate review findings |

---

## 1. Executive Verdict

The attached design is **directionally sound** for CNF (3× HikariCP pools, healthy-pool round-robin, management-owned status, monitoring thread, Multus static IPs, no NP application retry as currently written).

It does **not yet meet** a hard requirement of **no NP message failure during any Postgres server restart**, and it has a **transient unfairness / failure window** between the start of a restart and the moment the pool is removed from round-robin.

Those gaps are fixable without abandoning the borrow/return + 3-pool model — see §5–§6 and `HLD.md` §20.

---

## 2. Requirements Traceability

| Original / stated need | Attached design | Assessment |
| --- | --- | --- |
| Java + HikariCP | Explicit | **Met** |
| Multus static IP JDBC (mandatory) | `jdbc:postgresql://<cnpg-multus-ip>:5432/...` | **Met** |
| 1 Primary + 2 Replica pods | Three independent pools | **Met** |
| 16 workers + 1 management | Stated; +1 monitoring | **Met** |
| PreparedStatements | Driver cache (`prepareThreshold`, statement cache) | **Met** (different mechanism than app-owned PS per connection) |
| Equal distribution across DBs | Round-robin over UP pools | **Met in steady state**; **gap during restart transition** (§4) |
| Worker/monitor → management up/down events | DB Event Queue → Management | **Met** |
| Alarms + broadcast to SLP(s) | Management responsibilities | **Met** |
| No NP query retry on failure | Explicit; error returned to SLP | **Met as written** — **conflicts with “no message failure”** (§3) |
| Service OAM retry (max 2) | Stated | **Met** (OAM only) |
| Metrics (queries/connection, failed/available) | Broad metrics list | **Mostly met**; per-connection query counts need care under borrow/return |
| ≥ 5000 qps | Target stated | **Plausible** with warm pools + RR |
| Latency | **Confirmed: query response time &lt; 20 ms** | **Frozen SLO**; stakeholder timeouts 1–2 s remain **inconsistent** and must be redesigned (§7) |
| HA / connection re-establishment | Hikari recovery + monitor rejoin | **Partial** — recovery OK; in-flight NP loss not eliminated |
| Startup when DB down | `initializationFailTimeout = 0` | **Met** (good CNF choice) |
| App-managed list of 16 pre-prepared connections | Replaced by getConnection()/close() per query | **Intentional design change** — acceptable if PS cache + pool sizing proven |

---

## 3. Core Conflict: “No Retry” vs “No Message Failure”

| Statement | Source | Implication |
| --- | --- | --- |
| NP query failure is **not repeated** | Original brief + attached design | Failed NP lookup is returned to SLP / caller |
| **Minimal message loss** | Attached purpose | Softens absolute zero-loss |
| **No message failure** during Postgres restart | Current review ask | Hard zero-loss on the NP path |

**Architect conclusion:** Under a pure “fail once to SLP, never touch another pool for that request” rule, **Postgres restart will always produce some NP failures** for requests that already selected the restarting pool (and possibly for in-flight executes). Zero message failure is **impossible** without one of:

1. **Intra-request connection failover** — on infrastructure error, DB connector immediately tries the **next healthy pool** within the same request (same semantic as Service OAM’s max-2 alternate-pool attempts), **or**
2. **Perfectly coordinated drain** before every restart (planned only) so the pool is removed from RR with **zero in-flight** before Postgres stops accepting work.

Recommendation: treat (1) as **connection failover / pool reselection**, not “application retransmission of a business transaction,” and allow **up to 2 alternate pools** for NP on **classified infrastructure failures only** (socket closed, connection reset, broken pipe, pool acquire timeout, SQLState connection exceptions). Business SQL errors still fail once.

Without adopting (1) or (2), the design should **not** claim no message failure during restart.

---

## 4. Equal Load Distribution — Steady State vs Restart

### 4.1 What the attached design gets right

```
All 3 UP  → RR → ~33% / 33% / 33%
1 DOWN    → RR → ~50% / 50% / 0%
Recovered → RR → returns to ~33% / 33% / 33%
```

Workers never call `getConnection()` on a pool already marked DOWN → avoids systematic `connectionTimeout` storms. Pool size **17** (16 workers + 1 monitor) per instance correctly allows **all workers to converge on surviving pools** during outage.

### 4.2 Failure window (the real problem)

```
t0  Postgres restart begins (connections drop / refuse)
t1  Some workers still RR-select that pool (status still UP)
t2  Queries fail → error to SLP  ← MESSAGE FAILURE
t3  Failure events enqueued
t4  Management marks pool DOWN
t5  RR uses remaining pools only  ← equal LB resumes on survivors
```

During **[t0, t4]**:

- Load is **not** equally successful across instances (one third of selections may fail).
- Message failures occur even though post-DOWN behavior is correct.
- If monitor interval is **5 s** and traffic is skewed, detection can lag further when workers are the primary detectors.

### 4.3 Recovery window (secondary unfairness)

```
t6  Postgres accepts connections again
t7  Single successful SELECT 1 → Management marks UP (per attached note)
t8  Pool rejoins RR at full weight immediately
```

Risks:

- Early UP while pool still warming → acquire timeouts / partial failures.
- Sudden **+33%** share before minIdle connections are ready → latency spikes and possible message loss.
- Oscillation if restart is multi-phase (stop → start → recover).

---

## 5. Required Controls for Restart + Equal LB + No Message Failure

### 5.1 Pool state model (extend attached UP/DOWN)

| State | RR eligible? | Meaning |
| --- | --- | --- |
| **UP** | Yes | Full weight in round-robin |
| **DRAINING** | No | Planned or reactive quarantine; in-flight may finish; no new borrows |
| **DOWN** | No | Confirmed unavailable; monitor probes only |
| **RECOVERING** | No | Health OK but warming / consecutive probes not yet met |

### 5.2 Instant local circuit break (close t0→t4 gap)

Workers must **not** wait for the Management thread to dequeue an event before excluding a bad pool from RR:

1. On infrastructure failure, worker sets an **atomic per-pool quarantine flag** (or CAS status to DRAINING) **before** enqueueing the event.
2. Subsequent `selectHealthyPool()` skips quarantined pools **immediately**.
3. Management remains authoritative for alarms, SLP broadcast, and durable UP/DOWN transitions.

This preserves “Management owns status” for control-plane side effects while making the **data-plane RR view** fail-closed in microseconds.

### 5.3 Intra-request failover (close message-failure gap)

For NP lookups in the DB connector:

```
select pool (RR among UP)
borrow → execute
  on infra failure:
     quarantine pool (atomic)
     enqueue DOWN/DRAINING event
     if failoverAttempt < maxAlternatePools (2):
         select next UP pool (RR)
         borrow → execute   // same request, different pool
     else:
         return failure to SLP
  on success:
     return result
always close/return connection to its pool
```

Properties:

- Matches OAM “max 2 retries across 3 pools” capacity math.
- Preserves equal distribution **among pools still considered healthy**.
- Failed pool drops out of RR instantly → survivors absorb load **50/50** (or 100% if one remains).
- Does **not** retry on the same broken connection/pool in a tight loop.

### 5.4 Planned Postgres restart (ops-assisted zero loss)

For maintenance restarts, prefer coordinated drain:

1. Ops/CNPG hook or admin API → Management sets target pool **DRAINING**.
2. Wait until active borrows on that pool = 0 (or short grace).
3. Restart Postgres pod.
4. Monitor probes until **RECOVERING → UP** (see §5.5).
5. Rejoin RR at equal weight.

This is the only path that can approach **true zero** NP failure without relying on in-request failover for that event.

### 5.5 Recovery gating (preserve equal LB quality)

Do **not** mark UP on a single success if zero-loss / stable latency matter. Recommended:

- **N consecutive** successful health checks (e.g., N=2 or 3), and
- Hikari pool has reached **minimumIdle** connections (or active+idle ≥ threshold), and
- Optional: one synthetic prepared NP lookup on a borrowed connection.

Then transition RECOVERING → UP and rejoin RR so traffic returns to **~33/33/33** without a thundering herd of timeouts.

### 5.6 Timeout alignment during restart

If a pool is still UP but dying, `connectionTimeout=1000 ms` and `socketTimeout=1 s` allow workers to **block far beyond** a &lt;20 ms latency target and delay quarantine.

Recommendations:

| Setting | Attached | Recommended for &lt;20 ms path |
| --- | --- | --- |
| `connectionTimeout` | 1000 ms | **≤ 20–50 ms** for NP pools (fail fast → failover) |
| JDBC `socketTimeout` | 1 s | **≤ query budget** (e.g. 15–20 ms) or keep query timeout dominant |
| Statement query timeout | 2 s | **≤ 15–20 ms** for NP (2 s conflicts with &lt;20 ms SLO) |
| Monitor interval | 5 s | Keep 5 s for recovery probing; rely on **worker-driven quarantine** for fast DOWN |

Service OAM may use looser timeouts than the NP path.

---

## 6. Scenario Matrix (Postgres Restart)

| Scenario | Attached design behavior | Required behavior (this review) |
| --- | --- | --- |
| Planned restart of Replica-2 | Failures until monitor/worker mark DOWN; then 50/50 | Pre-drain → DRAINING → 50/50 with **no new failures**; restart; gated UP → 33/33/33 |
| Abrupt crash of Replica-2 | Some NP errors to SLP; then 50/50 | Atomic quarantine + **intra-request failover** → caller success if another pool healthy; RR excludes bad pool |
| Primary restart | Same; reads may still succeed on replicas if RR moves | Same controls; confirm product accepts replica reads during primary outage |
| Rolling restart of all 3 (bad ops) | Cascading failures | Guardrail: never DRAIN more than **one** pool at a time unless forced; alarm if &lt;2 UP |
| Flapping network to one Multus IP | Oscillating UP/DOWN; uneven success | Quarantine + recovery hysteresis; metrics on transitions |
| Restart during 5k qps peak | Timeout storms if pool still selected | Fast quarantine + short `connectionTimeout` + failover |

**Equal load after exclusion:** RR over remaining UP pools (attached §8/§17) — **retain**.  
**Equal load after recovery:** gated rejoin then full-weight RR — **add**.  
**No message failure:** quarantine + intra-request failover and/or planned drain — **add**.

---

## 7. Additional Findings (Non-Restart)

| ID | Finding | Severity | Note |
| --- | --- | --- | --- |
| F-01 | Latency SLO &lt;20 ms vs timeouts 1–2 s | **High** | Timeouts must be redesigned for NP path |
| F-02 | Doc math: “17 per pool” vs “Total max 48 (16×3)” | Low | Align to **51** if max=17, or document monitor sharing |
| F-03 | `prepareThreshold=3` delays server-side PS | Medium | Consider `1` for NP hot path after soak test |
| F-04 | Borrow/return vs original sticky 16 connections | Info | Acceptable; validate PS cache hit rate under failover |
| F-05 | Single-failure DOWN / single-success UP | Medium | Fast DOWN is good; single-success UP is risky (§5.5) |
| F-06 | Per-connection query metrics under pooling | Medium | Label by pool + connection UUID if required, or by pool only |
| F-07 | Event queue overload under mass disconnect | Medium | Coalesce per-pool; never drop first DOWN |
| F-08 | `validationTimeout=250` with `connectionTimeout=1000` | Info | OK ordering; both still high vs &lt;20 ms |
| F-09 | Latency SLO &lt;20 ms | Info | **Confirmed**; treat as normative for NP path soak tests |

---

## 8. Alignment with Prior HLD Defaults

| Topic | Prior `HLD.md` default | Attached design | Review recommendation |
| --- | --- | --- | --- |
| Pool max size | 6 per pool | **17 per pool** | **Adopt 17** if Postgres `max_connections` allows (full worker failover to one pod) |
| Connection ownership | Long-lived leased slots | Borrow per query | **Adopt borrow/return** + driver PS cache (simpler; matches attached) |
| Status hysteresis | N failures / M successes | Single probe | **Asymmetric**: fast quarantine DOWN; gated multi-probe UP |
| NP failover | None (fail to caller) | None for NP | **Add intra-request pool failover** to meet no message failure |
| Monitor interval | 1–2 s | 5 s | **5 s OK** if worker quarantine is instant |

---

## 9. Confirmation Asks (Updated)

Please confirm:

1. **NP intra-request failover** on infrastructure errors (max 2 alternate pools) — required for “no message failure”?  
2. Official latency SLO: **&lt; 20 ms** — **CONFIRMED**  
3. Pool sizing: **17 / 17 / 17** approved by DBA against Postgres capacity?  
4. Planned restart: will platform provide a **drain signal**, or rely solely on reactive quarantine + failover?  
5. Accept **DRAINING / RECOVERING** states beyond simple UP/DOWN?  
6. **Last UP pool also down**: fail fast with `NPDB_ALL_POOLS_UNAVAILABLE` + CRITICAL alarm (do not keep dead last pool in RR) — see HLD §6.4?

---

## 10. Document History

| Version | Date | Notes |
| --- | --- | --- |
| 0.1 | 2026-07-13 | Initial review of stakeholder CNF design; restart / equal-LB / no-message-failure analysis |
| 0.2 | 2026-07-13 | OD-07 closed: query response time requirement confirmed as &lt; 20 ms |
