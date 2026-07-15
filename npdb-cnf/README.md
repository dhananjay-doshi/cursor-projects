# npdb-cnf

Java common DB connector and process facades for Number Portability CNF Postgres access (HikariCP).

## Processes

| Process | Class | Threads | Pools |
| --- | --- | --- | --- |
| **DB Handler** | `com.npdb.cnf.handler.DbHandlerService` | 16 workers (`ThreadPoolExecutor` + `LinkedBlockingQueue`) + management + monitoring (in connector) | 3 × Hikari max/min **17** |
| **Service OAM** | `com.npdb.cnf.oam.ServiceOamDbService` | Main (caller) + 1 background bulk thread + management + monitoring | 3 × Hikari max/min **3** |

Both embed `com.npdb.cnf.connector.NpdbConnector` (common DB connector jar logic).

## Build

```bash
cd npdb-cnf
mvn -q test
mvn -q package
```

## Quick start (DB Handler)

```java
NpdbConnectorConfig cfg = NpdbConnectorConfig.builder(ProcessType.DB_HANDLER)
    .addEndpoint(new DbEndpointConfig("primary", "10.0.0.1", 5432, "m7np_db", "user", "pass", DbRole.PRIMARY))
    .addEndpoint(new DbEndpointConfig("r1", "10.0.0.2", 5432, "m7np_db", "user", "pass", DbRole.REPLICA))
    .addEndpoint(new DbEndpointConfig("r2", "10.0.0.3", 5432, "m7np_db", "user", "pass", DbRole.REPLICA))
    .build();

try (DbHandlerService handler = new DbHandlerService(cfg)) {
    handler.start();
    Future<NpLookupResult> f = handler.submitLookup("1234567890");
    NpLookupResult r = f.get();
}
```

## Quick start (Service OAM)

```java
NpdbConnectorConfig cfg = NpdbConnectorConfig.builder(ProcessType.SERVICE_OAM)
    .addEndpoint(...primary...)
    .addEndpoint(...replicas...)
    .bulkNetworkTimeoutMs(300_000) // 5 minutes for bulk
    .build();

try (ServiceOamDbService oam = new ServiceOamDbService(cfg)) {
    oam.start();
    oam.provisionSingle("INSERT INTO np_subscriber(...) VALUES (?, ?)", ps -> { ... });
    oam.submitBulkProvision("INSERT ...", batches).get();
}
```

## Pool status concurrency

Workers **read** status lock-free; management **writes**. See [`docs/POOL_STATUS_CONCURRENCY.md`](docs/POOL_STATUS_CONCURRENCY.md) for `CopyOnWriteArrayList` vs `AtomicReference` snapshot.

## Design docs

- [`HLD.md`](HLD.md) — working HLD
- Final stakeholder HLD (uploaded) — implementation source for pool sizes / timeouts / OAM retry
