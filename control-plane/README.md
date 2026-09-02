# OpenSearch Control Plane

A standalone service that exposes OpenSearch's control plane APIs — index, alias,
template, pipeline, and snapshot metadata operations — with a wire-identical HTTP
surface, generated from the
[OpenSearch API specification](https://github.com/opensearch-project/opensearch-api-specification).

## Status

Early. The full control plane surface is routed; the rest returns `501 Not Implemented`.

```
126 routes, 79 operation groups, 11 implemented, 68 stubbed
```

| namespace | routes | implemented |
|---|---|---|
| `indices` | 67 | `create`, `exists`, `delete` |
| `cluster` | 21 | — |
| `snapshot` | 16 | — |
| `_core` (info, ping, stored scripts) | 10 | `info`, `ping` |
| `ingest` | 5 | `put_pipeline`, `get_pipeline`, `delete_pipeline` |
| `search_pipeline` | 4 | `put`, `get`, `delete` |
| `dangling_indices` | 3 | — |

**The remaining 68 operation groups return `501 Not Implemented`** with a structured
error naming the operation group, so the routing surface is complete from day one and
clients get an honest answer rather than a 404.

```bash
./gradlew :server:run --args="--port 9400 --data-path /tmp/cp"

curl -XPUT localhost:9400/_ingest/pipeline/p -H 'Content-Type: application/json' \
     -d '{"description":"noop","processors":[]}'
curl localhost:9400/_ingest/pipeline?pretty
curl -XPUT localhost:9400/my-index -H 'Content-Type: application/json' \
     -d '{"settings":{"number_of_shards":2}}'
curl -XGET localhost:9400/_cluster/health   # 501, not yet implemented
```

## Where this sits

Three layers communicating loosely through a shared data store:

| layer | responsibility |
|---|---|
| **control plane** (this repo) | operations on the resources that exist — index, alias, template, pipeline, snapshot configuration; owns task lifecycle including retries and cleanup |
| **shard manager** | allocates resources to data nodes: reads registered index management tasks, sees which data nodes exist from their published heartbeats, writes per-node configuration |
| **data plane** | indexes and searches shards; "regular" OpenSearch plus a plugin that fetches configuration from the data store and synthesizes local cluster state in-process |

A create-index request therefore ends, from this service's point of view, at
"validate, write the index configuration, register an index management task". The
shard manager assigns shards; the data nodes consume their updated configuration and
do the actual create-shard work. No layer calls another directly.

This is why an `OperationHandler` returning a response is a complete operation:
nothing here blocks on shard work.

## How the API surface is defined

The HTTP surface is not hand-written. It is generated:

```
spec/opensearch-openapi.yaml     pinned bundle of the OpenSearch API spec
spec/control-plane-scope.yaml    which operation groups are in scope, and why
        |
        v  :codegen  (SpecCodegen)
        |
server/build/generated/.../GeneratedRoutes.java
```

Everything is keyed by the spec's `x-operation-group`. That extension is what
makes this tractable: OpenSearch models one logical operation as several
(method, path) pairs — `indices.put_alias` is eleven of them — and the operation
group collapses them back to one. So the route table has 128 entries but there
are only 81 things to implement.

### The claimed version decides the surface

`target_opensearch_version` in the scope file does two things at once, deliberately:

- the route table includes only operations and parameters available at that version, judged by
  `x-version-added` / `x-version-removed` in the spec
- `GET /` reports it as `version.number`

Codegen emits it as `GeneratedRoutes.TARGET_OPENSEARCH_VERSION` and `ClusterIdentity` reads it from
there, so there is one value rather than two that can disagree. **There is intentionally no runtime
flag for the reported version** — overriding it at startup would mean claiming to be a version the
route table was not generated for, which is exactly the inconsistency this removes.

Including an operation the target version does not have is a build error, not a warning: a service
reporting 3.9.0 must not answer a route a real 3.9.0 cluster rejects. At the current target of
`3.9.0` nothing is filtered out — nothing in the spec is gated above 3.5, and `x-version-removed`
appears only on responses in namespaces already out of scope. Lower the target and the filter bites:
at `2.11.0` it drops 165 of 823 routes.

### The scope file is the interesting part

`spec/control-plane-scope.yaml` enforces **total coverage** of the spec. Every
namespace must be classified, and within a reviewed namespace every operation
group must be individually marked included or excluded with a reason. Anything
unclassified **fails the build**.

That is deliberate. When you bump the pinned spec and OpenSearch has added an
API, the build breaks and tells you to make a decision, rather than silently not
implementing it.

The classification axis is *not* "control plane vs data plane". It is
**"answerable from cluster state and metadata alone"** vs **"requires shard data
or node-local runtime"** — which is what actually determines whether a standalone
service can serve a request. That is why `indices.get_mapping` is in scope while
`indices.stats`, `indices.refresh`, and `indices.analyze` are not, even though all
four live in the `indices` namespace.

Currently out of scope wholesale: `cat` and `list` (text/plain table rendering),
`security` (needs an auth backend), the index-management plugins (`ism`, `sm`,
`rollups`, `transforms`, `wlm`), and everything node- or shard-level. Each is one
line to re-enable.

`indices.clone` / `shrink` / `split` / `rollover` are in scope even though they
move data: the control plane validates and creates the target index metadata,
initiates the work, hands the shard copy off to the data plane, and owns the
overarching task lifecycle including retries and cleanup.

The `tasks` namespace is the one deliberate gap in that story. It is deferred, not
out of scope — exposing `_tasks` compatibly needs a task identity decision first,
because OpenSearch task IDs are `{node_id}:{task_num}` enumerated per-node over the
transport layer, and a standalone control plane has no node IDs to put there.

## Persistence

`KeyValueStore` (`org.opensearch.controlplane.store`) is the only seam through which
this service talks to anything else: an ordered key/value store of opaque byte
payloads. Keys are namespaced by resource, e.g. `ingest/pipeline/<id>`.

Every key carries a monotonically increasing revision, and writes can be conditional
on it. That is not speculative — `indices.create` has to fail with
`resource_already_exists_exception` rather than clobber an existing index
(`put(key, value, ABSENT)`), and `indices.update_aliases` / `put_mapping` are
read-modify-write cycles that lose updates without a compare-and-set. All the likely
backends support this directly: etcd compares `ModRevision`, DynamoDB uses a
conditional expression on a version attribute, S3 has conditional writes via
`If-Match` / `If-None-Match` on ETags.

Payloads are small — index and pipeline configuration, not documents — which is worth
weighing when considering an object store for this role.

**Implementations:**

| implementation | source set | notes |
|---|---|---|
| `FileSystemKeyValueStore` | main | local development and single-node. Durable, real CAS *within one process* |
| `InMemoryKeyValueStore` | test | not durable; deliberately unshippable |
| etcd / DynamoDB | *not built* | production |

`KeyValueStoreContractTests` is written purely against the interface and runs against both
implementations. An etcd or DynamoDB backend should extend it too: a behaviour asserted there is
one the control plane relies on regardless of backend.

### The filesystem store

One flat directory, one file per key, filename = strict percent-encoding of the key. Keys are
deliberately **not** mapped onto nested directories. Keys carry user input, so a segment of `..`
would escape the directory, and under tenancy the key prefix is the only thing separating tenants.
Encoding everything outside `[A-Za-z0-9_-]` turns `..` into `%2E%2E` and `/` into `%2F` — ordinary
filenames, no traversal possible, and no exposure to case-insensitive or reserved-name behaviour.
There is a test that writes `../sibling/secret` and asserts the sibling file is untouched.

Each file is an 8-byte big-endian revision followed by the payload. Writes go to a temp file in the
same directory, are fsynced, then `ATOMIC_MOVE`d into place, so a reader never sees a half-written
value. Revisions increment per key, which needs no separate counter to persist and survives
restarts.

Three limitations worth knowing:

- **CAS is process-local.** Conditional writes are serialised by an in-process lock, so two
  instances sharing a directory will race and lose updates. One process per directory, or use a
  store with real CAS. This is the main reason production wants etcd or DynamoDB.
- **`scanPrefix` is O(number of keys)** — it lists the directory and decodes every name. Fine for
  control plane volumes, not a substitute for an index.
- **Key length is bounded** by the filename limit (240 encoded characters). Exceeding it throws
  rather than truncating.

## Conformance

`conformance/run.sh` runs the spec repo's own test harness against this service — the same harness
used to validate OpenSearch itself. It replays YAML story files over HTTP and validates status codes
*and* response bodies against the spec, which is what proves the two surfaces agree rather than us
asserting they do.

```bash
# a real OpenSearch usually already holds 9200, so run the control plane elsewhere
./gradlew :server:run --args="--port 9400 --data-path /tmp/cp-conformance"

# in another shell
conformance/run.sh
conformance/run.sh --reference http://localhost:9200   # differential; see below
conformance/run.sh --spec-repo /path/to/opensearch-api-specification
conformance/run.sh -- --verbose          # anything after -- goes to the tester
conformance/run.sh --all                 # every story; a to-do list, not a gate
```

### Running against a reference OpenSearch

`--reference URL` replays the same stories against a real OpenSearch and compares, which separates
two failure modes that look identical otherwise:

| control plane | reference | meaning |
|---|---|---|
| pass | pass | agree |
| **fail** | **pass** | **a real divergence — this is the one that matters** |
| pass | fail | story depends on something out of scope, or a version difference |
| fail | fail | story or scope issue, not evidence against us |

Only the second row exits non-zero. Without a reference, a story that fails because it reaches an
unimplemented operation is indistinguishable from one that fails because we got the behaviour wrong,
and the whole point of the harness is telling those apart.

**Current state:** the ingest pipeline story passes against both the control plane and a real
OpenSearch 3.9.0 with no divergence. Two search pipeline stories have been added to the manifest but
not yet run.

Read that narrowly. The story exercises the happy path — put, get-by-id, get-all, delete — and its
epilogue accepts either 200 or 404, so it does *not* pin the error semantics: the three delete
not-found branches, the `processors`-required rejection, malformed-body handling, wildcards, and
comma-separated ID lists are covered by unit tests and by hand-verification against a live cluster,
but not yet by the harness. A passing conformance run is weaker evidence than it looks until the
stories reach those paths.

`conformance/stories.txt` lists the stories to run. Only stories whose every chapter is within the
implemented surface belong there. Add a line as each operation group lands. Right now that is one
three: the ingest pipeline story, and both search pipeline stories.

Stories run **in place**, one tester invocation each, rather than staged into a temp directory: the
tester validates each story's `$schema` field against the file's own location, so a copied story
fails validation before it ever runs. `--all` uses a single invocation and so also produces a
combined coverage report.

The harness creates and deletes real resources, so point the server at a throwaway `--data-path`.

## Multi-tenancy

One control plane instance serves many tenants. Isolation is **structural**: a handler
is handed a store already confined to the requesting tenant and never learns which
tenant it is serving, so cross-tenant access is not something it can get wrong. There is
no API through which to express it.

```
request headers
   │
   ▼  TenantResolver                    (headers -> TenantId, or 401)
   │
   ▼  OperationContextFactory
   │
   ▼  OperationContext{ tenant, TenantScopedKeyValueStore }
   │
   ▼  OperationHandler.handle(request, context)
```

Keys land under `tenant/<id>/`, so `ingest/pipeline/p` for two tenants is two distinct
rows. `scanPrefix` strips the prefix on the way back out, which is why `PipelineStore`
contains no tenancy logic at all — it sees exactly the key space it wrote.

Contrast the alternative, where every handler receives a tenant ID and is trusted to
incorporate it into every key. That works until one of 81 handlers forgets, and it fails
silently.

### Why not a URL prefix

Not merely because generated clients would not emit `/acme/_ingest/pipeline`. `GET
/{index}` is a real route, so `/acme/_settings` cannot be distinguished from the settings
of an index named `acme`. The ambiguity is unresolvable, so the path is out.

### Choosing a resolver

The property that matters is that **a caller cannot choose its own tenant**. If the
tenant comes from a header any client can set, then every authenticated caller can
address every other tenant's data and isolation depends on the transport being
trustworthy.

| resolver | tenant from | notes |
|---|---|---|
| `SingleTenantResolver` | fixed | single-tenant deployments and local dev |
| `HeaderTenantResolver` | `x-tenant-id` | **client-asserted.** Trusted proxy or `restrictedTo` only |
| *not yet built* | authenticated principal | **preferred.** Basic auth, bearer, SigV4 — all supported by vanilla clients, so no spec deviation |
| *not yet built* | SNI / `Host` | endpoint-per-tenant, zero client change, natural sharding boundary |
| *not yet built* | mTLS client certificate | transport surfaces the subject as a synthetic header |

Because data is under a tenant prefix even in single-tenant mode, moving to real
multi-tenancy is a resolver swap rather than a data migration.

`TenantId` validation is security-critical, not cosmetic: the value becomes a key prefix,
so a `/` in it could escape the namespace. Malformed IDs are rejected rather than
sanitised, since a value containing a slash is indistinguishable from an attempt to
escape.

**A warning for whoever writes the filesystem or object-store backend:** `KeyValueStore`
does no path normalisation, so `..` in a key is inert — just part of the key. An
implementation that maps keys onto paths *must* normalise or reject traversal sequences,
or the isolation above is defeated one layer down.

## Building

Requires JDK 21.

```bash
./gradlew build                      # generate routes, compile, test
./gradlew :server:run --args="--port 9400 --data-path /tmp/cp"
./gradlew :server:run                # start on 127.0.0.1:9200
./gradlew :server:run --args="--dump-routes --port 9200"
./gradlew :server:generateRoutes     # regenerate after editing the scope file
```

`:server:generateRoutes` also writes `server/build/reports/control-plane-scope.md`,
a readable breakdown of what is in scope and why.

### Offline builds

Dependency versions are pinned to what resolves from local caches, and
`settings.gradle.kts` lists `mavenLocal()` after `mavenCentral()` as a fallback,
so `./gradlew --offline build` also works. Gradle tries repositories in order, so
`mavenLocal()` only comes into play for modules `mavenCentral()` cannot supply —
in practice the JUnit 1.11.4 platform set, which is in `~/.m2` but not in the
Gradle module cache.

## Implementing an operation

Routing does not change. Put the handler in a package mirroring its spec namespace —
`handler.indices`, `handler.cluster`, `handler.ingest`, `handler.snapshot` — so the
package layout matches the scope file and the generated route table:

```
server/src/main/java/org/opensearch/controlplane/handler/
    OperationHandler.java          the SPI
    HandlerRegistry.java
    NotImplementedHandler.java
    ControlPlaneHandlers.java      <-- register here
    ingest/PutPipelineHandler.java <-- implementations go in namespace subpackages
    indices/CreateIndexHandler.java
```

```java
package org.opensearch.controlplane.handler.ingest;

public final class PutPipelineHandler implements OperationHandler {
    @Override
    public String operationGroup() {
        return "ingest.put_pipeline";   // must exist in the generated route table
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        String id = request.pathParam("id");
        // context.store() is already confined to the requesting tenant
        var pipelines = new PipelineStore(context.store());
        ...
    }
}
```

Handlers are **stateless singletons with no constructor arguments**. Everything
request-scoped arrives in the `OperationContext`, because the store a handler gets depends
on which tenant is calling. Register it in `ControlPlaneHandlers.registerAll` — the one
place that lists implemented operations:

```java
public static HandlerRegistry registerAll(HandlerRegistry registry) {
    return registry.register(new PutPipelineHandler());
}
```

Registration is explicit rather than classpath-scanned: the list is greppable, and
`HandlerRegistry.register` rejects an operation group that is not in the generated
route table, so a typo or a scope mismatch fails at startup instead of producing a
handler that silently never receives a request.

When handlers need further collaborators — a task registrar, a clock — add them to
`OperationContext` rather than to constructors. That keeps them out of the routing and
HTTP layers, and keeps anything request-scoped from being captured in a shared field.

### Where to start

`ingest.put_pipeline` / `get_pipeline` / `delete_pipeline` is the cheapest useful
vertical slice: pure metadata CRUD against the data store, no shard manager
involvement, no task to register. It exercises path parameters, body parsing,
persistence, and the error shapes end to end without any of the hard semantics.
`cluster.get_settings` / `put_settings` and `search_pipeline.*` are equivalent in
shape.

`indices.create` is the natural second step and the first one that needs the task
handoff — validate the request, resolve templates, write index configuration, then
register an index management task for the shard manager.

## Design notes

### The router is hand-written on purpose

`PathTrie` reimplements OpenSearch's path resolution rather than delegating to a
framework router. OpenSearch prefers a literal segment match at each level and
**backtracks** to the wildcard child when the literal subtree dead-ends. Framework
routers use different, usually non-backtracking, precedence — which diverges on
genuinely ambiguous OpenSearch paths like `/_alias/{name}` vs `/{index}/_alias`.
Matching the server is the whole point, so the trie is ours.

Consequence worth knowing: `GET /anything` routes to `indices.get`, because
`GET /{index}` is a registered route and any single segment is a valid index name.
That is OpenSearch's behaviour too.

### Placeholder names are per-route, not per-node

The spec names the same trie position differently across operations:
`indices.get_alias` is `GET /{index}/_alias` while `indices.rollover` is
`POST /{alias}/_rollover`. OpenSearch's own trie stores the name on the shared
node, so whichever template was registered last wins — which is why
`RestRolloverIndexAction` reads `request.param("index")` despite the documented
path saying `{alias}`.

This implementation binds placeholders from the matched route's own template, so
each handler sees the names its spec template declares. That diverges from the
server in internal naming only; placeholder names never appear on the wire and the
paths route identically.

### Three error shapes, all reproduced

OpenSearch emits three different error shapes and clients depend on all of them. All
three verified byte-exact against OpenSearch 3.9.0-SNAPSHOT:

| condition | status | body |
|---|---|---|
| handler failure | varies | `{"error":{"root_cause":[{...}],"type":...,"reason":...},"status":N}` |
| wrong method | 405 | `{"error":"Incorrect HTTP method for uri [...] ...","status":405}` + `Allow` |
| unknown path | 400 | `{"error":"no handler found for uri [...] and method [...]"}` |

Three things here are easy to get wrong, and I got two of them wrong first time:

- **`root_cause` is mandatory** in the structured shape, and it repeats the top-level
  cause *including extra fields* — a `parse_exception` carrying `property_name` has it
  in both places.
- **The unknown-path 400 has no `status` field at all.** It is the only response in the
  API that omits it, because `RestController.handleBadRequest` hand-builds the object
  with just `error`. The 405 path goes through `BytesRestResponse(RestStatus, String)`
  and does include `status`.
- **The unknown-path message is lower-case** (`no handler found`) while the 405 message
  is capitalised (`Incorrect HTTP method`).

The 501 shape is ours; OpenSearch never returns 501 because every route it registers is
implemented. It uses the structured form plus an `operation_group` field, which is legal
because the spec's `ErrorCause` schema permits additional properties.

## Verified against a live cluster

Confirmed against OpenSearch 3.9.0-SNAPSHOT. Bodies marked *byte-exact* were diffed
character for character against the server's response.

- **Unknown path** → `400`, lower-case `no handler found for uri [...] and method
  [...]`, **no `status` field**. Byte-exact.
- **Wrong method** → `405`, capitalised `Incorrect HTTP method for uri [...]`, with
  `status` present and a comma-separated `Allow` header. Byte-exact.
- **`+` in a path segment is a literal plus, not a space.** A pipeline created as
  `a+b` is retrievable as `a+b`; `a%20b` gives 404. So per-segment decoding must not
  apply form-encoding rules.
- **`GET /_ingest/pipeline` with no matches** → `404` with a bare `{}`. This holds even
  for a bare `GET` on a cluster with zero pipelines, and for `*` matching nothing.
- **`PUT /_ingest/pipeline/{id}` with no body** → `400 parse_exception`,
  `request body or source parameter is required`. Byte-exact (203 bytes).
- **`PUT` without `processors`** → `400 parse_exception`,
  `[processors] required property is missing`, plus `property_name: "processors"` at
  both the top level and inside `root_cause`. Byte-exact (255 bytes). **This is a spec
  bug:** the spec's `Pipeline` schema does not mark `processors` required. Worth
  reporting upstream.
- **`DELETE /_ingest/pipeline/{id}` not-found rules.** Subtler than they look, and the
  obvious reading is wrong. Mirrors `IngestService.innerDelete`:
  1. If ingest metadata has **never existed**, any delete is a no-op `200` — which is
     why deleting a nonexistent pipeline on a pristine cluster succeeds.
  2. Otherwise, nothing matched and the ID is exactly `*` → `200`. Match-all is exempt.
  3. Otherwise, nothing matched → `404 resource_not_found_exception`,
     `pipeline [<expr>] is missing`. **Partial wildcards are not exempt**: `logs-*`
     matching nothing is a 404 even though `*` is not.

  Note the trigger for (1) is the *presence of ingest metadata*, not the pipeline
  count: delete every pipeline and a subsequent delete-of-missing still 404s.
  `PipelineStore.METADATA_MARKER_KEY` replicates this. It is faithfully reproducing an
  implementation artifact leaking into the API — drop the marker if you would rather
  not carry the quirk forward.

## Still unverified

- Whether the 404-on-no-match behaviour of `GET /_ingest/pipeline` differs for an
  explicit comma-separated list where *some* IDs match and others do not (partial
  match is assumed to be a 200 here).
- Everything about the 76 operations that are still stubbed.
- **`indices.create` does not resolve templates.** A real create applies matching composable and
  legacy index templates, merging their settings, mappings and aliases with the request in a defined
  precedence order. None of that happens yet, so an index created here differs from one created on a
  cluster with matching templates. This is the largest gap in the operation and wants
  `MetadataIndexTemplateService`'s composition rules ported before it is trustworthy.
- **`shards_acknowledged` deliberately diverges.** A real cluster returns `true`; this returns `false`
  unless `wait_for_active_shards=0`, because it returns before any shard exists and will not claim
  otherwise. `false` is not an error signal in OpenSearch either — it is what a real cluster returns
  when the wait times out. Everything else about create, delete, and their error bodies is
  byte-identical to OpenSearch 3.9.0.
- Settings are stored unvalidated. A real cluster rejects unknown or malformed settings with 400.
- No conformance story covers the indices operations yet: every story under `tests/default/indices/`
  pulls in operations that are still stubbed.
- Whether the version filtering matters in practice. At `3.9.0` it drops nothing, so the mechanism is
  exercised only by lowering the target in a scratch scope file. It will start mattering the first
  time the pinned spec adds something above the claimed version.
- The `info` response is structurally identical to a live 3.9.0 cluster and every
  compatibility-relevant value matches. What differs is instance-specific and deliberate:
  `build_hash`/`build_date` are `unknown` unless injected at build time, and `build_type` is
  `controlplane` rather than `tar`.

The right way to keep closing these out is the spec repo's own test harness
(`tools/src/tester`), which runs ~412 YAML story files against a live cluster and
takes an `--opensearch-url`. Point it at this service, filtered to the operation
groups in scope, and it becomes a compatibility suite that proves the surfaces
match — far more systematic than hand-written curl probes.

## What is hard about the rest

[`docs/where-the-hard-parts-are.md`](docs/where-the-hard-parts-are.md) surveys the 73 stubbed
operation groups by what makes them difficult rather than by namespace. The short version:

- **Aliases will become their own keyed resource** rather than fields inside index metadata. That
  turns the canonical alias swap into a single-key compare-and-swap, so no multi-key transaction
  support is needed and object-store backends stay viable. It also turns alias lookup from a scan
  into a direct read.
- **Index expression resolution** (wildcards, `_all`, aliases, `expand_wildcards`) is the most reused
  logic in the `indices` namespace and should be written once, deliberately.
- **Template composition** wants porting from `MetadataIndexTemplateService` rather than reinventing,
  stopping short of the mapping engine.
- `cluster.pending_tasks` gets *easier* in this architecture: it is a read over our own task keys.
- **The service is stateless** — nothing outside the data store, no background threads — so it scales
  by adding instances and could scale to zero. Audited, and it holds today. The catch:
  `FileSystemKeyValueStore`'s compare-and-swap is process-local, so multi-instance needs etcd or
  DynamoDB.

## Bumping the spec

```bash
cd ../opensearch-api-specification && npm run merge
cp build/opensearch-openapi.yaml ../opensearch-control-plane/spec/opensearch-openapi.yaml
cd ../opensearch-control-plane && ./gradlew :server:generateRoutes
```

If OpenSearch added or removed an API, this fails with a list of what needs
classifying. `spec/SPEC_PROVENANCE.txt` records the spec repo commit the current
bundle came from.

## Layout

```
spec/                     pinned spec + scope definition
codegen/                  reads spec + scope, emits the route table
server/
  router/                 PathTrie, Router, Route, GeneratedRoutes (generated)
  rest/                   RestRequest, RestResponse, RestStatus, Json, ErrorResponses
  store/                  KeyValueStore, FileSystemKeyValueStore,
                          TenantScopedKeyValueStore
  tenant/                 TenantId, TenantResolver + implementations
  handler/                OperationHandler, OperationContext, HandlerRegistry,
                          ControlPlaneHandlers
  handler/pipeline/       ingest + search pipeline CRUD (one implementation, two kinds)
  handler/indices/        create, exists, delete
  http/                   Netty front end
server/src/test/
  store/                  InMemoryKeyValueStore (test-only, non-durable),
                          KeyValueStoreContractTests (run against both stores)
  tenant/                 TenantIsolationTests -- end-to-end isolation proof
conformance/              spec-harness runner + story manifest
```

## License

Apache 2.0.
