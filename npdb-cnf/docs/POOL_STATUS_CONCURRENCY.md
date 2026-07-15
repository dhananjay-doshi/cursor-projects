# Pool Status Concurrency: CopyOnWriteArrayList?

## Question

Can the 3 HikariCP pool statuses be maintained with `CopyOnWriteArrayList` to avoid locks between the management thread and worker / monitoring threads?

## Short answer

**Yes — it is a valid option** for this design, because:

- Pool count is small and bounded (**≤ 3**)
- Status writes are **rare** (UP/DOWN transitions)
- Status reads are **very frequent** (every NP query on the RR path)

Per the finalized HLD, **only the management thread writes** pool status; workers and monitoring **read** and publish events. That write-rare / read-heavy pattern matches `CopyOnWriteArrayList` well.

## How it fits the HLD

| Thread | Access |
| --- | --- |
| Management | Sole writer: replace or update entries on UP/DOWN |
| Workers (16) | Lock-free readers while selecting next UP pool |
| Monitoring | Reader (+ event producer); does not mutate status |

Workers never call `list.set(...)` / `add` / `remove` — they only iterate a snapshot view of UP pools.

## Recommended shapes (pick one)

### Option A — `CopyOnWriteArrayList<PoolRuntimeState>` (simple)

```text
CopyOnWriteArrayList with 3 fixed slots (or list rebuilt on change)
Management: cowList.set(index, newState)  // copy-on-write
Workers: for (PoolRuntimeState s : cowList) if (s.isUp()) ...
```

Pros: simple, no explicit lock on read path.  
Cons: each write copies the array (fine for N=3); iterating yields a snapshot that may briefly lag the latest write (acceptable).

### Option B — `AtomicReference<ImmutableSnapshot>` (**preferred**)

```text
record Snapshot(List<PoolRuntimeState> pools, int upMask) {}
AtomicReference<Snapshot> ref
Management: ref.set(new Snapshot(...))
Workers: Snapshot s = ref.get();  // single volatile read
```

Pros: one atomic publish of consistent view (states + RR mask together); clearest memory semantics.  
Cons: tiny bit more code than COW list.

### Option C — `AtomicInteger` eligibility bitmask + separate name map

```text
bit0=primary, bit1=r1, bit2=r2
Workers RR using mask only (fastest)
Management updates mask + richer status for alarms/CLI
```

Pros: fastest RR hot path.  
Cons: status details still need a side structure for MnpCtl display.

## What this module implements

`PoolStatusRegistry` uses **Option B** (`AtomicReference` to an immutable snapshot) as the primary structure, with an API that could be backed by COW list equivalently.

Rationale vs raw `CopyOnWriteArrayList`:

- Publishing **all three** pool states in one snapshot avoids torn reads (e.g. seeing pool0 UP and pool1 mid-update inconsistently across two `get` calls).
- Same lock-free read property workers need.
- N=3 makes snapshot allocation trivial.

If the team prefers COW list for familiarity, swap the registry internals — **do not** let workers write the list.

## Anti-patterns

| Avoid | Why |
| --- | --- |
| `synchronized` around every RR selection | Contends 16 workers at 5k qps |
| Workers calling `list.set` to mark DOWN | Violates HLD (“management sole owner”); races with monitor recovery |
| Mutable `PoolRuntimeState` fields without safe publication | Readers may see stale/torn fields |

## Bottom line

`CopyOnWriteArrayList` **can** maintain the 3 pool statuses safely for lock-free worker reads. This codebase uses an **`AtomicReference` snapshot** for the same concurrency model with slightly stronger consistency on multi-pool reads. Both are sound; neither replaces the management-thread ownership rule in the HLD.
