# Candidate backends

Rough outlines of four more `KeyValueStore` backends — DynamoDB, a relational database, an OpenSearch index,
and S3 — and what supporting them would change about the interface. None of the four is built. The existing
backends are etcd, ZooKeeper and the filesystem store.

**Status:** the kv-store side of the interface changes is done. The revision contract, consistency rules
and limits are in the `KeyValueStore` Javadoc, and every existing backend enforces and passes them, including
`actualRevision` as best-effort (`RevisionConflictException.UNKNOWN`). Two side effects:

- The filesystem store now hashes keys too long for a filename. Production-length keys failed there before.
- The etcd store now pages its scans. One unpaged range failed above gRPC's 4 MiB message limit.

The control plane now stays inside the limits too (`f8e3073`):

- Names that become key segments are capped at 255 UTF-8 bytes. That covers aliases, pipelines, templates,
  stored scripts and repositories.
- Index and alias documents are checked against the value limit before anything is written.
- A `LimitExceededException` that still reaches the router answers `400`.

One gap remains, recorded in `control-plane/README.md`: a resume's `reset_settings` go into a task written
after the index document, and their size is not bounded.

## TL;DR

- **All four look feasible.** In order of fit: relational, DynamoDB, OpenSearch, S3.
- **The biggest interface question is revisions, and the answer is to relax the contract, not the backends.**
  The contract suite requires revisions to increase *across keys*; no consumer relies on that. Dropping it
  is what lets DynamoDB and S3 avoid a store-wide counter.
- **Two limits the interface never stated now need stating:** a maximum value size (960 KiB, set by
  ZooKeeper), which DynamoDB covers by overflowing large values to S3, and a maximum key length (512 raw
  bytes), which S3, DynamoDB and OpenSearch all cap.
- **Read consistency and failover durability need writing down.** etcd and ZooKeeper provide both without
  being asked; three of the four candidates need a specific configuration to.
- **Scan cost is the real operational risk for DynamoDB and S3.** Every consumer polls, and the control
  plane scans full index documents on every request. That is a consumer and caching question rather than a
  reason to change `scanPrefix`.

## What consumers actually rely on

A survey of the three services, which is what the "lowest common denominator" has to satisfy. File
references are relative to the repository root.

| Property | Relied on? | Evidence |
|---|---|---|
| Revision equality as a change signal | **Yes**, everywhere | `RevisionCache`, `RevisionSilence` (liveness and lease expiry are "revision unchanged for T") |
| `put` returns exactly the revision later reads show | **Yes** | `LeaseManager` compares `current.revision() != mine` |
| A revision is never reused for a key, even across delete and recreate | **Yes** | interface Javadoc; the incident it records |
| Writing identical bytes still changes the revision | **Yes** | `AbstractStoreAssumptionsIT`; rules out content hashes such as S3 ETags |
| The same `long` in every process, surviving JSON | **Yes** | `configRevision` goes from `tenant/<t>/indices/<name>` into index settings and node reports, and is compared by the supervisor (`TaskCompletion`) |
| Revisions ordered within a key | No | no `<`, `>` or max on a revision anywhere |
| Revisions ordered across keys | No | same; the counters that *are* ordered (lease epoch, primary term, assignment version) live inside values |
| Linearizable conditional writes | **Yes** | leases, task claims, assignments, primary-term counters |
| Read-your-writes, and reading the winner after a lost create | **Yes** | `ClusterUuid`, `CreateIndexHandler`, CAS retry loops |
| Bounded read staleness | **Yes** | stale heartbeat reads beyond ~20 s declare a healthy node lost |
| Multi-key atomicity | No | disclaimed in `supervisor/KEY_LAYOUT.md`; writes are ordered instead |
| `scanPrefix` as a snapshot | No | ZooKeeper tears and that is tolerated; no key may be lost or invented when the key set is stable |
| Empty value distinct from absent key | **Yes** | `*-initialized` marker keys |
| Prefixes not aligned to `/` | Contract only | every production prefix ends in `/`, but `tenants/` and `tenant/` are adjacent |
| `RevisionConflictException.actualRevision()` | No | never read |
| Watches | No | no production caller; every loop polls |

Load, steady state: about 0.2 writes/s per data node and 1.5 + P writes/s per supervisor holding P pools.
Reads dominate. Each supervisor scans a dozen or more prefixes every 2 s, including fleet-wide
`nodes/{registration,heartbeat,assignment}/`. Each data node scans its tenants' `status/indices/` every 5 s.
The control plane scans `tenant/<t>/indices/` (full documents with mappings) and `aliases/` on every
index-expression request.

Sizes: values are UTF-8 JSON. `nodes/shards`, `nodes/assignment` and `status/indices/<uuid>` are budgeted
at 512 KiB each (`supervisor/SUPERVISOR_KEYS.md`). `indices/<name>` holds full mappings and has no cap.
Keys are at most six segments. Index names are capped at 255 bytes, but alias, template, pipeline, script
and repository names go into keys unvalidated.

## Interface implications

### 1. Revisions: never reused per key; no cross-key order

The Javadoc already says revisions "need not be global". The contract suite disagrees:
`aDeletedKeysRevisionIsNotHandedToAnotherWrite` asserts that a write to key `c` gets a revision greater than
deleted key `b` had. That is a store-wide order. etcd, ZooKeeper, a SQL sequence and a single-shard
OpenSearch index all provide one. DynamoDB and S3 have no cheap way to: it would take one counter item or
object that every write goes through.

Proposed contract, which is what consumers need:

1. Every successful write gives the key a revision it has never had, including across delete and recreate,
   and including when the bytes are identical.
2. Revisions strictly increase **per key**. Consumers do not need this, but it is what makes rule 1
   reasonable to implement and test, and every design below provides it.
3. `put` returns the revision that reads will show.
4. A revision is positive and below 2^53, so it survives any JSON parser that reads numbers as doubles.
   `configRevision` already crosses JSON. Every current backend fits, and the OpenSearch encoding below is
   sized to.
5. Nothing is promised across keys. Two keys may share a revision.

That means moving `aDeletedKeysRevisionIsNotHandedToAnotherWrite` into `FileSystemKeyValueStoreTests`. The
failure it guards against — recovering a store-wide counter from surviving keys — is specific to
counter-based backends.

Without a global counter, **per-key hybrid revisions** work for DynamoDB and S3:

- `next = max(previous + 1, now in microseconds)`, about 1.8 × 10^15 today, under 2^53 until the 2250s.
- Every write is internally conditional on the previous revision, unconditional `put` included. Otherwise
  two concurrent writers could pick the same microsecond and issue one revision to two different values.
  ZooKeeper's `put` already loops this way.
- A delete leaves a **tombstone** carrying the last revision, so a recreate continues from it.
- Tombstones are collected after a horizon, such as a day. A recreate after collection falls back to the
  clock. That is safe as long as clock skew between writers is far smaller than the horizon; the hazard is
  a clock running a day behind.

### 2. A stated maximum value size

The interface says payloads are "small" but sets no limit. The limits differ:

| Backend | Limit |
|---|---|
| ZooKeeper | ~1 MB (`jute.maxbuffer`) |
| etcd | 1.5 MiB request default |
| **DynamoDB** | **400 KB per item** |
| S3, relational, OpenSearch | effectively unlimited at these sizes |

Proposed: the interface states a size every backend must accept, with a contract test at that size.
**960 KiB** fits:

- ZooKeeper's `jute.maxbuffer` is 1 MiB − 1 byte for the *whole request*, path and frame included, so a
  full 1 MiB cannot be promised.
- The supervisor's 512 KiB budgets (`supervisor/SUPERVISOR_KEYS.md` §5) already sit inside it, and are
  already enforced by refusing the write.

Over the limit, a backend throws a dedicated exception rather than a generic failure, so a consumer can
tell "too big" from "store down". That leaves DynamoDB to cover the gap itself (see below). It also
needed `indices/<name>` capped in the control plane, which now checks index and alias documents before
writing them.

### 3. A stated maximum key length

| Backend | Limit |
|---|---|
| OpenSearch | 512-byte document ID (hashable, see below) |
| DynamoDB | 1,024-byte sort key |
| S3 | 1,024-byte object key |
| Postgres | ~2.7 KB per btree entry |
| MySQL | 3,072 bytes per index key |

Proposed: the interface guarantees **raw keys up to 512 UTF-8 bytes**, and each backend fits that inside
its native limit:

- OpenSearch hashes long keys into the document ID.
- ZooKeeper's escaping can triple non-ASCII bytes, and its limit is far larger.
- DynamoDB and S3 have 1,024 bytes, or less where a partition prefix is taken out of the sort key.

Longer keys are rejected with a clear error rather than truncated. The longest production key today is
about 400 bytes (`tenant/<128-byte tenant>/indices/<255-byte name>`). That holds because the control plane
caps every user-supplied name that goes into a key at 255 bytes, as for index names.

### 4. Consistency and durability, written down

These go in the interface Javadoc and, where testable, in the contract suite:

- Conditional writes are linearizable per key.
- Through one store instance, reads see that instance's own writes, and never go backwards.
- After a conditional write loses, a read sees the winner (`ClusterUuid`, `CreateIndexHandler`).
- Across instances, reads may lag by a bounded amount: well under the ~20 s at which a stale heartbeat
  reads as a lost node.
- Scans are not snapshots.
- An acknowledged write survives failover of the backend.

The cross-instance rule is weaker than "reads reflect every acknowledged write" on purpose. That stronger
rule is what etcd provides, but ZooKeeper does not: without `sync()`, which this backend never calls, a
follower can serve a read from behind the leader. One process can therefore miss another's acknowledged
write briefly — a REST `PUT` then `GET` through two control-plane instances, for example. Promising the
strong rule would mean adding `sync()` to ZooKeeper reads, at a round trip each. Promising the weak one
costs nothing today and admits more backends.

Each candidate below has a specific setting that breaks one of these rules: eventually consistent DynamoDB
reads (no read-your-writes), an asynchronous SQL replica (failover loss), OpenSearch's refresh interval
(scans miss writes), or a multi-region table (conditional writes not linearizable).

### 5. Scan cost

Nothing needs to change in `scanPrefix` for correctness. What changes is cost. etcd, ZooKeeper,
relational and OpenSearch scans are cheap enough to poll. DynamoDB charges per byte read on every scan.
S3 needs one request per value. The levers, in the order to pull them:

1. **Backend-internal caching.** S3 can list cheaply and re-fetch only objects whose ETag moved; see below.
2. **Consumer caching.** The control plane rebuilding `IndexAbstractions` from full mappings on every
   request is the expensive pattern. A revision-keyed cache there, which is a control-plane change, would
   help every backend.
3. **A keys-and-revisions scan** (`scanRevisions(prefix)`, no values) so revision caches can skip fetches.
   Not proposed yet: on DynamoDB a projection does not reduce the read cost of a `Query`, so it only helps
   S3, which caching already covers.

### 6. Smaller points

- **`actualRevision` becomes best-effort.** Nothing reads it, and S3 cannot report it without another
  request. Document it as possibly `ABSENT` when unknown, rather than costing every lost race a round trip.
- **Credentials do not go in the `--store` URI.** It lands in process listings and logs. Cloud backends use
  the AWS default credential chain; the others take a credentials file or environment variable named in
  the URI.
- **Connection counts.** A connection per data node is fine for etcd and HTTP backends, but a relational
  database with 1,000 nodes needs a pooler such as PgBouncer or RDS Proxy.
- **Packaging inside the data plane.** A backend jar is dropped into the plugin directory and loads next to
  OpenSearch's own libraries. Anything that duplicates a core class, such as an unshaded Jackson or Apache
  HttpClient, fails OpenSearch's jar-hell check. This constrains the AWS SDK's HTTP client choice and the
  OpenSearch backend's JSON handling.
- **Watches are unchanged.** Postgres has `LISTEN/NOTIFY`, DynamoDB has Streams and S3 has event
  notifications. None is needed, and polling remains the contract.
- **Consumer-level tests.** The supervisor's `AbstractStoreAssumptionsIT`, `AbstractLeaseIT` and
  `AbstractConvergenceIT` test more of the assumptions above than the contract suite does. A new backend
  should pass them as well. Moving the store-level assumptions into this build's test fixtures would put
  them where backend authors look.

## Backend sketches

### Relational (Postgres, MySQL/MariaDB)

The best fit. The database does more than is asked of it, and nothing about the interface strains.

```sql
CREATE TABLE kv (
  k        BYTEA  PRIMARY KEY,   -- VARBINARY(1024) on MySQL: byte comparison, never a collation
  v        BYTEA  NOT NULL,
  revision BIGINT NOT NULL
);
CREATE SEQUENCE kv_revision;     -- MySQL: a one-row counter table, or a sequence on MariaDB
```

| Operation | SQL |
|---|---|
| `put(k, v, ABSENT)` | `INSERT … ON CONFLICT DO NOTHING RETURNING revision`; MySQL: plain `INSERT`, duplicate key → conflict |
| `put(k, v, r)` | `UPDATE kv SET v=?, revision=nextval(…) WHERE k=? AND revision=?`; zero rows → conflict |
| `delete(k, r)` | `DELETE … WHERE k=? AND revision=?` |
| `scanPrefix(p)` | `WHERE k >= p AND k < successor(p)` on bytes; sorted client-side into Java string order |

- **Revisions** come from a global sequence. Sequences are not transactional, so a rollback leaves a gap,
  which is permitted. No tombstones are needed. This even passes the current cross-key contract test.
- **Key column must compare bytes.** MySQL's default collation is case- and accent-insensitive, so `Index`
  and `index` would be one key. Postgres text compares under the database locale. Bytes avoid both, and a
  range bound is simpler than escaping `LIKE`.
- **Isolation:** single-row conditional statements are atomic under READ COMMITTED, and under MySQL's
  REPEATABLE READ because `UPDATE` reads the latest committed row. No SERIALIZABLE needed.
- **Failover is the risk.** With asynchronous replication, a promoted replica can lack commits that
  readers already saw. Values then roll back, and a sequence can reissue a revision that was already seen,
  which is the ABA case rule 1 exists to prevent. The requirement is synchronous replication, or a managed
  service whose storage replicates synchronously (RDS Multi-AZ, Aurora). Reads go to the primary.
- **URI:** `postgresql://host:5432/db?table=kv` and `mysql://…`, mapped to JDBC; credentials from a file.
  JDBC drivers are self-contained, so data-plane packaging is easy.
- **Tests:** Testcontainers has Postgres and MySQL modules, with high fidelity.

### DynamoDB

A good fit for conditional writes. It needs work on value size and on how keys are partitioned.

- **Item:** partition key `p`, sort key `s`, `v` (binary), `rev` (number), and optionally `tomb`.
- **Conditional writes** are native: `attribute_not_exists(s)` for `ABSENT`, or `rev = :expected`.
  `ReturnValuesOnConditionCheckFailure` returns the current item without another read.
- **Revisions:** per-key hybrid revisions with tombstones (implication 1). The alternative, an atomic
  counter item, caps the store at about 1,000 writes/s and makes every write two requests. Tombstones could
  be collected with DynamoDB TTL, but whether a TTL deletion can race a recreate needs checking.
  A collector that does a conditional delete is the safe default.
- **Reads** must use `ConsistentRead=true`, on both `GetItem` and `Query`. Global secondary indexes cannot
  provide it, so scans run on the base table.
- **Partitioning:** `Query` needs one partition-key value, and each partition serves about 3,000 RCU and
  1,000 WCU. Two options:
  1. **`p` = the key's first two segments** (`tenant/acme`, `nodes/heartbeat`), `s` = the rest. Every
     production prefix with at least two whole segments is one `Query`. Shorter prefixes, such as
     `tenants/`, need a `Scan` of the whole table. That is ruinous every 2 s, so it would need either a
     layout rule or a per-prefix exception.
  2. **Hierarchical, like ZooKeeper:** `p` = the parent path, `s` = the last segment, plus directory
     items. Any prefix works, at one `Query` per directory level.

  This is the main design decision. Option 1 is simpler if the key layout commits to it.
- **Value size:** 400 KB is below the 512 KiB budgets and the 960 KiB guarantee. The preferred answer is
  **overflow to S3**. A value that fits stays inline. Larger values are written as follows:
  1. Write the value to S3 at a never-reused path, such as `<hash of key>/<random UUID>`.
  2. Make the usual conditional write to DynamoDB, carrying a pointer instead of the bytes.

  This works because the DynamoDB item stays the only thing that is ever compare-and-set, so atomicity and
  revisions are untouched. S3 only ever receives new objects, so it needs no conditional writes, which
  sidesteps every open question in the S3 sketch. S3 reads are strongly consistent, so a pointer that has
  been read always resolves.
  - **Lost CAS:** delete the object just written.
  - **Crash between the steps:** leaves an unreferenced object. A sweeper deletes objects under the
    overflow prefix that no item references and that are older than a day.
  - **Overwritten object:** deleted only after a delay, so a reader holding the old pointer can still fetch
    it. A reader that does find it gone re-reads the item.

  Compression is still worth having on top, because DynamoDB bills reads by bytes. The alternative, chunks
  plus a head item in one `TransactWriteItems`, avoids a second service but makes every large write a
  transaction and every large read several items.
- **Cost:** a strongly consistent read costs 1 RCU per 4 KB per item read. A control-plane request that
  scans a tenant's 1,000 indices at 20 KB each reads about 20 MB. That is the scan-cost problem in
  implication 5, at its worst here.
- **Multi-region:** conditional writes on an eventually consistent global table are evaluated per region,
  and the last writer wins across regions. That breaks leases. Use a single-region table, or verify the
  multi-region strong-consistency mode's support for conditional writes before relying on it.
- **URI:** `dynamodb://table?region=us-east-1`; credentials from the default chain.
- **Packaging:** AWS SDK v2 shades its Jackson. Use the URL-connection or JDK HTTP client rather than the
  Apache or Netty client, to stay clear of OpenSearch's own copies.
- **Tests:** DynamoDB Local in Docker. Its fidelity for conditional writes and transactions is good, but
  its throughput and partition behaviour are not representative.

### OpenSearch index

Plausible, and attractive for a deployment that is "only OpenSearch". The work is in revision encoding,
search visibility and validating its consistency.

- **Layout:** one index (default `clusterless-kv`), **one primary shard**, replicas across zones, mapping
  `dynamic: false`. Fields: `key` (keyword, for prefix queries and sorting) and `v` (binary, not indexed).
  The document ID is the key, or a SHA-256 of it when the key exceeds 512 bytes, with `key` holding the
  full value.
- **Conditional writes:** `op_type=create` for `ABSENT`; `if_seq_no` plus `if_primary_term` otherwise, and
  the same on `DELETE`. A 409 means a conflict. Every response carries the new `_seq_no` and
  `_primary_term`.
- **Revisions:** `_seq_no` alone is not safe. After a primary failover, operations that never reached a
  replica are rolled back and their sequence numbers reissued under the new term, possibly to a value that
  had been readable. So the revision packs both: `primary_term << 40 | seq_no`. That is about 10^12
  operations per term and 8,192 terms, within 2^53. Exhausting either means reindexing into a fresh index
  with revisions bumped past the old ones. One shard makes revisions store-wide, which config scale allows.
  Sequence numbers keep moving across deletes, so no tombstones are needed.
- **Visibility:** a `GET` is real-time, but search only sees refreshed documents. Writes use
  `refresh=wait_for`, so an acknowledged write is visible to scans. That costs up to one refresh interval
  per write, acceptable at these write rates.
- **Scans:** a `prefix` query on `key`, sorted by `key`, paginated with `search_after`, with
  `seq_no_primary_term=true`. A point-in-time is not needed, because scans are not snapshots.
- **Consistency needs validation, not assumption.** Writes are replicated to all in-sync copies before
  they are acknowledged, and primary terms fence a deposed primary, so CAS should be linearizable. But a
  read can observe a write that is later rolled back, and an isolated old primary can serve stale reads
  briefly. Before leases are trusted to it, it should pass the supervisor's lease ITs under a
  partition-and-kill nemesis. OpenSearch does not advertise linearizable CAS, and the Jepsen history of its
  ancestor is the reason to check.
- **Packaging:** inside the data plane, an official OpenSearch client or a bundled Jackson collides with the
  node's own classes. Use REST over the JDK `HttpClient`, with JSON either hand-written (the documents are
  tiny) or taken from the host where one is present.
- **Tests:** an official Testcontainers OpenSearch module exists.
- **Not recursive:** the donor cluster is an ordinary OpenSearch cluster with a cluster manager. It cannot
  be backed by this store.

### S3

Technically possible since S3 added conditional writes. It is the slowest and most expensive of the four,
and it has the most open questions.

- **One object per key.** The body is a small header (revision, tombstone flag) followed by the value. The
  revision is also set as `x-amz-meta-revision`, so a `HEAD` can read it.
- **Why the revision goes in the body:** an ETag hashes the content, so rewriting earlier bytes would bring
  back an earlier ETag. Putting the revision in the body makes every write's content, and so its ETag,
  unique. The ETag can then serve as the atomicity token and as a change detector.
- **Conditional writes:** `If-None-Match: *` for `ABSENT`. Otherwise a `HEAD` to check the revision, then a
  `PUT` with `If-Match: <etag>`, failing with 412. This is the same read-check-then-versioned-write shape as
  ZooKeeper, with the ETag as the version.
- **Revisions:** per-key hybrid revisions. A delete is a conditional overwrite with a tombstone, which also
  covers conditional delete. Tombstones are collected later with a conditional `DELETE`. **Whether
  conditional `DeleteObject` is available on general-purpose buckets needs verifying.** Without it,
  collection can race a recreate and delete a live key.
- **Scans:** `ListObjectsV2` is strongly consistent, takes raw string prefixes (an exact fit for the
  contract), and returns ETags but not metadata. So a scan is a `LIST` plus a `GET` per value. The backend
  keeps an ETag → (revision, value) cache and re-fetches only objects whose ETag moved. That turns a
  steady-state scan into one `LIST` page. Tombstones are filtered out after fetching.
- **Latency and cost:** tens of milliseconds per request, and a CAS is two requests. A supervisor listing a
  dozen prefixes every 2 s is around half a million `LIST` requests a day before any `GET`. That is small
  money, but not nothing, and it multiplies by data nodes. S3 Express One Zone cuts latency at the cost of
  single-zone durability.
- **URI:** `s3://bucket/prefix?region=…`; credentials from the default chain.
- **Tests:** MinIO or LocalStack, but **an emulator's conditional-write behaviour is not evidence about
  S3's**. The contract suite has to run against real S3 before this backend is trusted.

## Comparison

| | Relational | DynamoDB | OpenSearch | S3 |
|---|---|---|---|---|
| Conditional writes | native, per row | native, conditions | native, seq_no + term | `If-Match` / `If-None-Match` |
| Revision source | global sequence | per-key hybrid + tombstones | packed term + seq_no | per-key hybrid + tombstones |
| Passes today's cross-key test | yes | no | yes | no |
| Value limit vs 960 KiB guarantee | fine | **400 KB: overflow to S3** | fine | fine |
| Key limit | ~2.7–3 KB | 1,024 B | 512 B (hashed beyond) | 1,024 B |
| Read-your-writes needs | reads on the primary | `ConsistentRead` | `refresh=wait_for` | native |
| Scan cost when polling | low | **per byte read** | low | **per value, cached** |
| Main risk | async failover | partitioning, item size | consistency under partition | conditional delete, emulator fidelity |

## Order of work

1. **Interface changes first, since every backend needs them.** Done (`ea27608`):
   - the revision contract, moving the cross-key test into the filesystem tests
   - the value-size and key-length guarantees, with contract tests
   - the consistency and durability rules
   - `actualRevision` as best-effort

   And done in the control plane (`f8e3073`): caps on `indices/<name>` size and on user-supplied names in
   keys.
2. **Relational backend:** lowest risk, and a check that the contract suite is written in backend-neutral
   terms.
3. **DynamoDB**, after deciding how keys are partitioned.
4. **OpenSearch**, with the lease nemesis test as the gate.
5. **S3**, after confirming conditional deletes, and against real S3.
