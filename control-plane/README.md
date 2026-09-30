# OpenSearch Control Plane

A standalone service that exposes OpenSearch's control plane APIs — index, alias,
template, pipeline, and snapshot metadata operations — with a wire-identical HTTP
surface, generated from the
[OpenSearch API specification](https://github.com/opensearch-project/opensearch-api-specification).

## Status

Early. The full control plane surface is routed; the rest returns `501 Not Implemented`.

```
119 routes, 75 operation groups, 55 implemented, 20 stubbed
(58 / 17 when the capabilities file declares script languages, contexts and grok patterns)
```

| namespace | routes | implemented |
|---|---|---|
| `indices` | 61 | `create` (composable + legacy template application), `exists`, `delete`, `put_mapping`, `put_settings`, `open`, `close`; the read set — `get`, `get_settings`, `get_mapping`, `get_field_mapping`, `resolve_index`; the full alias set; template CRUD + both simulate operations; `create_data_stream`, `delete_data_stream` |
| `cluster` | 21 | component template CRUD, `get_settings`, `put_settings`, `pending_tasks`, weighted routing (`get`/`put`/`delete_weighted_routing`) |
| `snapshot` | 16 | repository registration: `create_repository`, `get_repository`, `delete_repository` |
| `_core` (info, ping, stored scripts) | 10 | `info`, `ping`, `put_script`, `get_script`, `delete_script`, `get_script_languages`†, `get_script_context`† |
| `ingest` | 5 | `put_pipeline`, `get_pipeline`, `delete_pipeline`, `processor_grok`† |
| `search_pipeline` | 4 | `put`, `get`, `delete` |
| `ingestion` | 2 | `pause`, `resume` — *controlling* pull-based ingestion. `get_state` is out of scope |

**† Three operations are registered only when the capabilities file declares their data**:
`get_script_languages`, `get_script_context` and `ingest.processor_grok`. Each is a pure passthrough, so
with nothing declared there is no operation to perform and it keeps its stub — answering
`{"types_allowed":[],"language_contexts":[]}`, `{"contexts":[]}` or `{"patterns":{}}` would tell a client
the cluster has no scripting languages, no scripting contexts or no grok patterns at all, which is a
confident wrong answer where a 501 is an honest one. Every other capability-dependent behaviour degrades
to *not validating*; these three have nothing to degrade to, because the capability data **is** the
response. Only the first also validates something (a stored script's `lang`); the other two are
report-only.

So the implemented count depends on configuration. Generate the file rather than writing it:
`tools/dump-capabilities.sh http://localhost:9200 > capabilities.yaml`.

**The remaining groups return `501 Not Implemented`** with a structured
error naming the operation group, so the routing surface is complete from day one and
clients get an honest answer rather than a 404.

```bash
../gradlew :server:run --args="--port 9400 --data-path /tmp/cp"

curl -XPUT localhost:9400/_ingest/pipeline/p -H 'Content-Type: application/json' \
     -d '{"description":"noop","processors":[]}'
curl localhost:9400/_ingest/pipeline?pretty
curl -XPUT localhost:9400/my-index -H 'Content-Type: application/json' \
     -d '{"settings":{"number_of_shards":2}}'
curl -XGET localhost:9400/_cluster/health   # 501, not yet implemented
```

## Where this sits

Three planes communicating loosely through a shared data store:

| plane | responsibility | lifetime of its work |
|---|---|---|
| **control plane** (this repo) | **short-lived CRUD only.** Operations on the resources that exist — index, alias, template, pipeline configuration, and snapshot repository *registration* | completes within the request |
| **management plane** | longer-running orchestration, as several logical services that may or may not be colocated: shard allocation (the "shard manager"), repository cleanup and cloning, health checking, and the task lifecycle — retries, leases, cleanup | minutes to hours |
| **data plane** | hosts the physical indices and serves customer interactions with that data: "regular" OpenSearch plus a plugin that fetches configuration from the data store and synthesizes local cluster state in-process | per request, on the data |

A create-index request therefore ends, from this service's point of view, at
"validate, write the index configuration, register an index management task". The
management plane assigns shards; the data nodes consume their updated configuration
and do the actual create-shard work. No plane calls another directly.

This is why an `OperationHandler` returning a response is a complete operation:
nothing here blocks on shard work.

### The service this is meant to be

Worth stating, because it decides questions the layering alone leaves open. The model
is closer to **OpenSearch Serverless than to a managed-hardware offering**: the
operators run pools of nodes on hardware they own, and a tenant says what they need
rather than what to run it on. Contrast a managed service where the customer chooses
and pays for specific instance types, and so has a claim to say which of them their
index sits on.

The audience is **internal** — a search platform team operating this as a service for
product and business teams, not a public cloud offering. So the aim is that a tenant
*need not* care where their data lives, rather than that they cannot find out:
secrecy is not a design goal, and a tenant who sees behind the curtain has found
something not useful to them rather than something they were denied.

What tenants keep is the part that matters — their own indices, their own mappings,
their own business problems. They know their business and want search as a solution;
the operators know OpenSearch and want running that solution to be painless. An
autoscaling component that reacts to supervisor and monitoring signals and orders
hardware is intended but not yet scoped, and it is the thing to anticipate when
classifying anything with a capacity number in it. See
[`docs/tenants-and-operators.md`](docs/tenants-and-operators.md).

### "Short-lived CRUD only" is the line, and it decides scope

The sharper version of the older test ("answerable from cluster state and metadata
alone"), and it settles cases that one left ambiguous. If an operation cannot
finish inside its own request, it does not belong here — it belongs to the
management plane, and this service's part of it is to validate, write metadata, and
register a task.

That is why **snapshot repository registration is here and snapshot administration
is not**. Registering a repository is a single key. Cleaning one up, cloning a
snapshot, or verifying a repository are long-running jobs against object storage —
and although they touch no running data node, "not the data plane" does not make
them the control plane's. See
[`docs/where-the-hard-parts-are.md`](docs/where-the-hard-parts-are.md).

It also explains a gap this service has today rather than excusing it:
`cluster.pending_tasks` reports a list that only grows, because marking a task
complete is the management plane's half of the handoff and there is no management
plane yet.

*On the name:* "management plane" comes from networking, where the classic split is
data / control / management. Be aware the conventional networking meaning is not
quite this one — there it tends to mean the operator-facing configuration and
monitoring surface, which is closer to what this repo does. Used here it means the
long-running orchestration layer, which is what the definition above says and what
matters.

### The second line: tenants own what they want to exist, operators own the machines

The line above asks *can this service serve the request?* There is a second one,
and it asks *may this caller make it?* **A setting is the tenant's if it describes
what they want to exist; the operator's if it describes the machines, or the timing
of changes to them.**

The axes are independent, and the gap between them matters: an operation can be pure
metadata, finish inside its own request, and still not belong here. Vanilla
OpenSearch needs no such rule, because it assumes the party that runs a cluster and
the party that uses it are the same person — one `cluster.*` namespace, one admin
role. Here they are different parties.

In practice nearly everything this service does is the tenant's. The exceptions are
worth knowing, because one of them is already wired: **the weighted-routing
validator's entire input is operator-owned** — `AwarenessConfiguration` reads only
`cluster.routing.allocation.awareness.attributes` and `…force.<attribute>.values`,
and `zone` is a fact about a rack in a building. The supervisor has verified that
nothing there reads those settings, so a tenant setting them changes nothing about
where copies go.

The rule is `../supervisor/DESIGN.md` §25, with one companion clause
this surface forced: **a resource naming a neighbouring service or cloud
infrastructure may belong to either party, and which one is a property of the
individual registration rather than of the resource type.** A managed service's
automated-backup bucket and a customer's own bucket are both snapshot repositories
registered through the same API, so what is needed is a mechanism for an operator to
register one a tenant cannot modify — visible and read-only, since hiding it costs
real complexity and buys nothing for an internal service — rather than a ruling on
who owns repositories.

The sweep of this surface — including the three decommission-awareness groups,
index-level node filters, the quota mechanism this service does not have, why the fix
for awareness cannot land as a rejection on its own, and why shard count is a
question the tenant should probably not be asked — is
[`docs/tenants-and-operators.md`](docs/tenants-and-operators.md).

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

**`dangling_indices` is excluded on principle rather than on layering**, and it is
the only exclusion of that kind. Those three operations *are* control-plane-shaped,
and we still do not want them. A dangling index is one whose data exists on a node
while its metadata is absent from cluster state — two sources of truth having
diverged — and the API exists to reconcile that by hand. Here index metadata lives
in the shared store and nodes derive their configuration from it, so there is one
source of truth and no way for the two to disagree. **If a dangling index ever
appears, the distributed system is incorrect**, and the answer is to fix it rather
than to ship a tool for importing the wreckage.

`indices.rollover` is in scope even though it moves data, on the same terms as
`indices.create`: the control plane validates, creates the target index metadata and
registers a task. The copy itself and the task's lifecycle — retries, cleanup,
reporting — belong to the management plane, which is what keeps it short-lived CRUD
from this side.

**`indices.clone` / `shrink` / `split` used to sit beside it and no longer do.** They
are excluded on the ownership axis rather than the layering one: **a tenant cannot
trigger them directly, because they are operator-initiated** — by a human operator or
eventually by the autoscaling service. `shrink` and `split` are heavy enough to want
hardware provisioned for the purpose rather than nodes serving queries, and shard
count is supply rather than a tenant's demand. A tenant that wants a split still gets
one by asking; what is gone is the direct trigger, not the outcome. `rollover` stays
because it is index lifecycle, not shard-count surgery.

One consequence to know: an excluded operation has no route, so these answer the
generic `400 no handler found` rather than a message explaining who to ask. That is
the honest answer available today — a helpful one needs a place to point at, and
there is no operator-facing surface yet. See
[`docs/tenants-and-operators.md`](docs/tenants-and-operators.md).

The `tasks` namespace is the one deliberate gap in that story. It is deferred, not
out of scope — exposing `_tasks` compatibly needs a task identity decision first,
because OpenSearch task IDs are `{node_id}:{task_num}` enumerated per-node over the
transport layer, and a standalone control plane has no node IDs to put there.

## Persistence

`KeyValueStore` (`org.opensearch.controlplane.store`) is the only seam through which
this service talks to anything else: an ordered key/value store of opaque byte
payloads. Keys are namespaced by resource, e.g. `ingest/pipeline/<id>`.

### Backends are pluggable jars

The interface, the two JDK-only implementations, and the shared contract test suite live in **a separate
build**, [`../kv-store`](../kv-store), which has no dependencies at all. It is its own
build because the supervisor (management plane) shares it: two implementations of the same revision
semantics against the same keys is how two planes would silently disagree about state.

```
../kv-store
  store-api          KeyValueStore, the provider SPI, FileSystemKeyValueStore,
                     InMemoryKeyValueStore + KeyValueStoreContractTests (test fixtures)
  store-zookeeper    \ separate jars, each bringing its own client library
  store-etcd         /
this build
  :server            depends on store-api only; names no backend
```

It is consumed as a Gradle **composite build** — `includeBuild("../kv-store")` in
`settings.gradle.kts` — so editing both services together needs no publish step.

An operator selects one by URI scheme:

| scheme | module | status |
|---|---|---|
| `file:///path` | `store-api` | bundled. Real CAS *within one process* only — one process per directory |
| `zk://host:2181/root` | `store-zookeeper` | verified against ZooKeeper 3.9 |
| `etcd://host:2379/prefix` | `store-etcd` | verified against etcd 3.7 |

```bash
--store file:///var/lib/control-plane        # bundled
--store zk://a:2181,b:2181/controlplane      # needs store-zookeeper on the classpath
--store etcd://a:2379,b:2379/controlplane    # needs store-etcd on the classpath
```

**"needs … on the classpath" is literal, and it catches everyone once.** `:server` does not depend on any
backend — that is what makes them pluggable — so a plain `../gradlew :server:run` has only the filesystem
store and `--store zk://…` fails with *no store provider for scheme [zk]*. `--help` lists what the running
classpath actually provides, so a short list means a missing jar rather than a missing feature.

To run against one, name its coordinate:

```bash
../gradlew :server:run -PstoreJars=org.opensearch.controlplane:store-zookeeper:0.1.0-SNAPSHOT \
  --args="--port 9400 --store zk://localhost:2181/controlplane"
```

Or set it once, in `gradle.properties`:

```properties
storeJars=org.opensearch.controlplane:store-zookeeper:0.1.0-SNAPSHOT,\
          org.opensearch.controlplane:store-etcd:0.1.0-SNAPSHOT
```

`storeJars` takes coordinates rather than a shorthand like `zk` on purpose: a shorthand would mean this
repository's build naming the backends it knows about, which is the coupling the provider seam exists to
remove. Outside Gradle it is just a classpath — put the jar on it.

**Deploying**, the same rule applies: the distribution ships `:server` plus `store-api`, and an operator
adds whichever backend jar and its client library they intend to use. That is the point — a deployment on
etcd need not carry Curator, and one on ZooKeeper need not carry gRPC.

Backend jars are not dependencies of `:server`; they go on the runtime classpath and are found by
`ServiceLoader`. Each brings its own client library, so adding one adds its transitive dependencies —
Curator for ZooKeeper, gRPC for etcd — which is the reason they are separate modules rather than
optional code in `:server`.

`--data-path <dir>` still works and is exactly `--store file://<dir>`, resolved through the same seam
rather than bypassing it. `--help` lists the schemes the running classpath actually provides, and an
unknown scheme says which ones it found — the likely cause being a missing jar.

**Why `ServiceLoader` here when [Design notes](#design-notes) rejects it for handlers.** The reasons
given there are good and simply do not transfer. A handler always exists in this repo, so an explicit
list can name every one; a backend lives in a jar that may not be present, and the point is that
`server` need not know it exists. The failure modes differ too: a missed handler registration is silent
(the operation answers 501), where a missing store provider is fatal at startup and reports what it did
find. And selection stays explicit where it counts — the operator names a scheme, so the backend in use
is a configuration fact rather than whichever jar was found first.

### Writing a backend

Implement `KeyValueStore` and `KeyValueStoreProvider`, declare the provider in
`META-INF/services/org.opensearch.controlplane.store.KeyValueStoreProvider`, and extend
`KeyValueStoreContractTests`. See that repository's README; the suite is published as test fixtures. Two of its
assertions constrain the design more than they look:

- **`emptyValuesRoundTrip`** — an empty value must be distinguishable from an absent key, because marker
  keys rely on it. A backend may not use "has no data" to mean "is not a key".
- **`dotDotIsAnOrdinaryKeySegment`** — `..` is an ordinary key segment and must never resolve. Any
  backend mapping keys onto a path-like namespace has to encode it; ZooKeeper rejects `.` and `..` as
  znode names outright, and `scanPrefix` is specified over key prefixes rather than path boundaries, so
  a non-segment-aligned prefix like `tenant/acme/x` has to work too.

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

The harness lives in a clone of
[opensearch-api-specification](https://github.com/opensearch-project/opensearch-api-specification), with
`npm install` run in it; `--spec-repo` names that clone and is required.

```bash
# a real OpenSearch usually already holds 9200, so run the control plane elsewhere
../gradlew :server:run --args="--port 9400 --data-path /tmp/cp-conformance"

# in another shell
SPEC=/path/to/opensearch-api-specification
conformance/run.sh --spec-repo $SPEC
conformance/run.sh --spec-repo $SPEC --reference http://localhost:9200   # differential; see below
conformance/run.sh --spec-repo $SPEC -- --verbose   # anything after -- goes to the tester
conformance/run.sh --spec-repo $SPEC --all          # every story; a to-do list, not a gate
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
OpenSearch 3.9.0 with no divergence. That covers 4 of the spec's 710 paths.

The two search pipeline stories were tried and removed: their payloads use `normalization-processor`
and `score-ranker-processor`, which come from the neural-search plugin, so a plain cluster answers
400 and they cannot pass on either side. The differential mode classified them correctly as "fails on
both — story or scope issue, not a divergence", which is what that classification is for. The lesson
is about curation: **checking that a story's paths are in scope is not enough — run it against a
reference cluster before adding it.**

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

## Data plane capabilities

The OpenSearch API specification describes the union of everything in the distribution, every bundled
plugin included. So **"in the spec" does not mean "available in this deployment"**: a pipeline using
`normalization-processor` is spec-valid, but a cluster without the neural-search plugin rejects it
with 400, and nothing in the spec marks that processor as plugin-provided.

Accepting such a definition and letting it fail at execution time is a poor experience, so the control
plane can be told what its data plane supports and refuse up front.

```bash
tools/dump-capabilities.sh http://localhost:9200 > capabilities.yaml
../gradlew :server:run --args="--port 9400 --capabilities capabilities.yaml"
```

**Generated, not hand-written.** A cluster already reports exactly this under `GET /_nodes` —
`ingest.processors` plus the three `search_pipelines.*` lists — so the file is produced from the
cluster the control plane fronts. A hand-maintained list would drift, and drift here means rejecting
definitions that would have worked: a false rejection breaks a working deployment, while a false
acceptance only defers an error. Re-run the script when plugins change.

Where a processor is present on only some nodes, the generator excludes it — the control plane should
not accept a definition whose success depends on where it runs.

Without `--capabilities`, processor types are not validated and startup says so. That is the default
because there is no accurate list to ship: `DataPlaneCapabilities` is an interface precisely so that
the durable answer — the shard manager publishing capabilities into the data store — is a change of
implementation rather than of design.

The two kinds report an unknown type differently, and both wordings are reproduced:

| | exception | reason |
|---|---|---|
| ingest | `parse_exception` | `No processor type exists with name [x]`, plus a `processor_type` field |
| search pipeline | `illegal_argument_exception` | `Invalid processor type x` |

## Settings

A client may write a setting three ways, and OpenSearch accepts all three. It stores **one**:

```
{"number_of_shards": 2}                 ─┐
{"index.number_of_shards": 2}            ├─►   index.number_of_shards = "2"
{"index": {"number_of_shards": 2}}      ─┘
```

That canonical form — a sorted map of dotted keys to *strings* — is what
`org.opensearch.controlplane.metadata.Settings` holds, and what this service stores, for index
metadata and for all three template kinds. The shapes seen on the wire are renderings chosen on the
way out: nested by default, dotted under `flat_settings=true`.

Verified against 3.9.0, and none of it is in the spec:

- **Templates normalise too**, not just index metadata. A template written
  `{"settings": {"number_of_shards": 2}}` reads back `{"settings": {"index": {"number_of_shards": "2"}}}`
  for legacy, composable and component alike, and `_simulate_index` agrees.
- Normalisation happens **before** validation — sending `{"totally_bogus": 1}` is rejected as
  `unknown setting [index.totally_bogus]`, a key the client never wrote.
- Numbers and booleans stringify. **Lists stay lists of strings.** **`null` stays null**, not `"null"`.
- **A dotted key beats the nested spelling of the same setting in either document order.** It is not
  last-writer-wins; `Settings.flatten` reproduces it by descending into objects before applying leaves.
- `flat_settings` is **per operation**: `GET /_settings` and legacy `GET /_template` honour it,
  `GET /_index_template` ignores it. That matches which operations declare the parameter in the spec.

**Why flat is the stored form** rather than the nested shape a client sees: composing a template means
overriding settings key by key, and on a flat map that is a `put` — exact, and independent of which
shape either side used. Merging the nested documents structurally only worked when both sides happened
to spell a setting the same way. A component saying `{"index": {"number_of_replicas": 9}}` beneath a
template block saying `{"number_of_replicas": 5}` produced *two keys for one setting*, and
`_simulate_index` reported both where a real cluster reports one.

### Script languages

The one data-plane capability with a first-class source of its own, and the reason
`get_script_languages` could be implemented at all. Which languages exist depends on installed
modules — `painless` is core, `expression` and `mustache` are modules — so the control plane cannot
know. But unlike processor types (reported only inside `GET /_nodes`) or repository types (reported
nowhere), a cluster exposes exactly this as `GET /_script_language`, which is both where the
capabilities file gets it and the operation this service then serves.

```yaml
script_languages:
  types_allowed: [inline, stored]
  language_contexts:
    painless: [aggs, field, filter, score, ...]
    mustache: [template]
```

`tools/dump-capabilities.sh` reads it alongside `GET /_nodes`. The section is optional: a capabilities
file generated before it existed still loads, and the query is allowed to fail — an older cluster, or
one behind a proxy that blocks it, simply yields no section rather than an empty one.

Declaring it does two things: `get_script_languages` starts answering instead of returning 501, and a
stored script whose `lang` the data plane does not have is rejected with the message a real cluster
uses. Without it, both degrade — the read stays stubbed and any `lang` is accepted.

### The settings registry

The same argument, applied to a registry that moves faster. A real cluster rejects an unknown setting
with `settings_exception`, `unknown setting [index.totally_bogus]`, and rejects a *static* setting
changed on an open index. Neither check is possible without knowing what settings exist and which are
dynamic — and that cannot be hardcoded here. There are 219 built-in index settings, OpenSearch adds
more on a semi-regular basis, and plugins register their own.

So it comes from the data plane, which emits it directly — the whole workflow is a curl:

```bash
curl 'localhost:9200/_cluster/settings_registry?pretty' -o settings_registry.yaml
../gradlew :server:run --args="--port 9400 --settings-registry settings_registry.yaml"
```

```yaml
version: "3.9.0"
index:
  - {key: "index.number_of_shards",            kind: exact,  properties: [Final, IndexScope]}
  - {key: "index.number_of_replicas",          kind: exact,  properties: [Dynamic, IndexScope]}
  - {key: "index.analysis.",                   kind: prefix, properties: [IndexScope]}
  - {key: "index.routing.allocation.include.", kind: prefix, properties: [Dynamic, IndexScope]}
cluster:
  - {key: "cluster.max_shards_per_node",       kind: exact,  properties: [Dynamic, NodeScope]}
  - {key: "cluster.remote.*.seeds",            kind: affix,  properties: [Dynamic, NodeScope]}
```

**Its own file, not folded into `capabilities.yaml`** — a reversal of what this section first proposed,
and worth saying why. The one-file argument was that two snapshots drift. But the two come from
different endpoints with different refresh cadences, the registry is ~180 kB against the processor
list's few hundred bytes, and the endpoint emits a standalone document that a plain curl already puts
in exactly the right shape. Making the dump script merge them would add tooling to avoid a second
`--` flag. A capabilities file carrying the same lists under a `settings` key is still accepted, for a
deployment that prefers one file; `--settings-registry` wins when both are given.

**Three entry kinds, and the reason is not fussiness.** OpenSearch splits its registered settings into
ordinary keys and *complex matchers*, and eight of the 221 index settings are the latter — including
`Setting.groupSetting("index.analysis.", ...)`, `index.similarity.`, and the three
`index.routing.allocation.*` groups. Anything beneath those prefixes is valid, without limit. Affix
settings put the variable part in the middle (`cluster.remote.*.seeds`); 45 cluster settings are that
shape and no index setting is. A registry that only knew exact keys would reject every custom
analyzer — precisely the false rejection this mechanism exists to prevent.

The emitter guarantees an invariant the consumer relies on, verified across all 1022 entries of a real
registry: **an `affix` key always contains a `*`, and a `prefix` key never does and always ends with a
`.`** That is why `index.routing.allocation.include.` is a prefix rather than an affix — it is built by
`Setting.prefixKeySetting`, whose key has no `*` and matches exactly as a prefix does.

`properties` carries OpenSearch's `Setting.Property` values verbatim. `InternalIndex` or
`PrivateIndex` means a user may not supply the setting at all and outranks the rest; otherwise
`Dynamic` is dynamic and anything else is fixed once set. Note that is *"not `Dynamic`"* rather than
*"`Final`"* — a setting with neither property is static, and keying off `Final` alone would let most
static settings through.

**Status:** implemented on both sides, and `indices.put_settings` is the first consumer. The data plane exposes
`GET /_cluster/settings_registry` (pending upstream in OpenSearch),
and `SettingsRegistry` / `DeclaredSettings` / `PermissiveSettings` consume it. A real 3.9.0 cluster
reports **221 index settings and 801 cluster settings**; the file is ~180 kB and loads in about 100 ms.
Consumed by `indices.put_settings` and `cluster.put_settings`, which are the reason it exists.

**A note on heterogeneous clusters.** The capabilities generator *excludes* a processor present on only
some nodes, since the control plane should not accept a definition whose success depends on where it
runs. A setting present on only some nodes is a different thing — a misconfiguration, usually a plugin
installed unevenly — and should be reported loudly rather than silently dropped.

### Cluster settings: two scopes, uninterpreted

`persistent` and `transient` are stored separately and neither is interpreted. The control plane's job
is to remember what a client set, in the scope it set it in.

**Which one wins is the data plane's question**, and it resolves it by merging with transient taking
precedence — the semantics OpenSearch already has. So there is no need for a precedence rule here, and
not having one is what keeps this a store rather than a policy. A data plane that implements only the
merged view is still correct; it just cannot report the two apart, which is a read-side concern.

One document per tenant rather than one key per setting, because an update is naturally
read-modify-write over the whole set. That makes it a single-key compare-and-swap — genuinely atomic on
any backend, the same property that made aliases their own resource, reached from the opposite
direction.

Each tenant has its own cluster settings, since the store a handler gets is already tenant-confined.
Same choice as `cluster_uuid` being per tenant: each tenant sees a coherent virtual cluster of its own.

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
../gradlew build                      # generate routes, compile, test
../gradlew :server:run --args="--port 9400 --data-path /tmp/cp"
../gradlew :server:run                # start on 127.0.0.1:9200
../gradlew :server:run --args="--dump-routes --port 9200"
../gradlew :server:generateRoutes     # regenerate after editing the scope file
```

`:server:generateRoutes` also writes `server/build/reports/control-plane-scope.md`,
a readable breakdown of what is in scope and why.

### Offline builds

Dependency versions are pinned to what resolves from local caches, and
`settings.gradle.kts` lists `mavenLocal()` after `mavenCentral()` as a fallback,
so `../gradlew --offline build` also works. Gradle tries repositories in order, so
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

## Considered divergences

Places where this service deliberately does not match OpenSearch. Each is a decision with a reason,
not unfinished work — that lives under [Still unverified](#still-unverified). The distinction matters:
without it, a deliberate choice reads as a bug and someone "fixes" it.

| behaviour | OpenSearch | here | why |
|---|---|---|---|
| `human` parameter | reformats durations and sizes | accepted and ignored | presentation is a client concern; responses should stay machine-parseable, and the parameter is a deprecation candidate upstream |
| `error_trace` | traces any error | traces only exceptions that escaped a handler | an explicitly-checked failure is an expected outcome, not an exceptional one; its `reason` already says what was wrong |
| `shards_acknowledged` on create | `true` | `false` unless `wait_for_active_shards=0` | we return before any shard exists and will not claim otherwise; `false` is what a real cluster returns when the wait times out |
| `version.build_type` | `tar` | `controlplane` | this is not a tar build of OpenSearch |
| `version.build_hash` / `build_date` | real values | `unknown` unless injected at build time | a fabricated hash defeats the only purpose the field has |
| voting config exclusions | 200 | 400, route absent | a stateless service has no cluster-manager quorum of its own; consensus belongs to etcd or DynamoDB |
| unknown processor types | rejected | accepted until `--capabilities` is supplied | no accurate built-in list can be shipped; see [Data plane capabilities](#data-plane-capabilities) |
| `cluster_uuid` | one per cluster | one per tenant | each tenant sees a coherent virtual cluster of its own |
| two write indices for an alias | 500 `illegal_state_exception` | 400 `illegal_argument_exception`, same reason | a request the client got wrong is a client error; reporting it as a server fault tells the caller to retry something that can never succeed. The type follows the status rather than being left inconsistent with it |
| nested settings key order | `HashMap` order — neither sorted nor as supplied | sorted | reproducing it would mean reproducing Java's hash iteration order. JSON object order is not semantic, and the harness compares parsed structures. The *flat* rendering is sorted on a real cluster too, so that one agrees |
| `index.version.created` | written into a created index's settings | absent | it records the Lucene and OpenSearch version the index was built with, which this service does not know. `index.replication.type` is *not* in this category — it is a real index setting, accepted on create and defaulted to `DOCUMENT` |
| restating an immutable mapping parameter at its default | accepted — the mapper resolves defaults, so `index: true` on a text field is a no-op | rejected | we store mappings as sent and have no defaults to resolve against, so an immutable parameter the stored mapping never declared cannot be told apart from a real change. Erring towards rejection is deliberate: accepting a change to how data is indexed leaves a mapping the data plane cannot honour, while a false rejection is immediate and is worked around by sending only the fields being added |
| the `from [x]` clause in a mapping conflict | `Cannot update parameter [index] from [true] to [false]` | `Cannot update parameter [index] to [false]` when the old value was never declared | same cause — there is no honest previous value to name, so the clause is dropped rather than invented. Byte-exact whenever the stored mapping did declare it |
| `shards_acknowledged` on reopen | `true` after awaiting shards | `true` | the one place this is *more* optimistic than `indices.create`'s deliberate `false`. On close it is honest — there are no shards left to wait for — and a request that changed nothing reports `false` as a real cluster does. Reopen genuinely awaits shards upstream, so `true` there is the same claim create declines to make |
| `time_in_queue` / `time_in_execution` on `pending_tasks` | a string beside each `_millis` field, emitted *unconditionally* rather than under `human` | omitted; only the `_millis` fields | the same call as `human`, in a harder place. Formatting a duration as `1.5s` is a presentation decision, and the server does not know the caller's locale or precision needs. The spec marks neither field required, so the response stays valid, and a client with `1500` can render it however it likes |
| `line` / `col` on a stored-script parse error | reported from the XContent parser's token location | omitted | this service parses with a different parser on a different pass, so any position it produced would be plausible and wrong — worse than absent. The `reason` already names the offending field |
| mapping key order | reordered by the mapping engine | as declared | the field dictionaries `properties` and `fields` come back sorted, but the other mapping keys follow the mapper's own emission order — `dynamic` before `properties`, neither declared nor alphabetical. Sorting the dictionaries would match a simple mapping and silently diverge on a richer one, while *looking* faithful. Reproducing it means the mapping engine |
| metadata fields in `get_field_mapping` | `_index`, `_routing`, `_doc_count`, … returned under a wildcard | declared fields only | they come from the mapping engine's field registry, which is version- and plugin-dependent; a hardcoded copy would drift the way a hardcoded settings registry would |
| `shards_acknowledged` on ingestion pause/resume | reports whether every shard's poller acted | always `false` | the spec marks it required, so it cannot be omitted, and this returns before any poller has seen the change. Same call as `indices.create`: the honest answer is that we did not wait |
| `failures` on ingestion pause/resume | per-shard failures, grouped by index | never present | a shard failure is not something a metadata write can observe. The field is optional in the spec, so the response stays valid |
| `include_defaults` on `get_settings` | a sibling `defaults` block, ~2.6 kB per index | accepted and ignored | **the control plane owns the settings a user explicitly set; computed values belong to the layer that computes them.** A data plane node can already answer `/{index}/_settings?local=true` with the fully resolved settings, defaults included, so putting a settings registry here would duplicate that — and a duplicated registry drifts. Same reasoning as [Data plane capabilities](#data-plane-capabilities) |
| `PUT` weighted routing with no body | **500** `null_pointer_exception`, the reason naming `WeightedRouting.attributeName()` | 400 `parse_exception`, `request body is required` | the body is never parsed, so the request object stays null and `validate()` dereferences it. Reproducing a crash is not compatibility, and the message leaks a server-internal class name at a status that tells the caller to retry. Third place OpenSearch answers 5xx for a plain client error, after `source` without `source_content_type` and an unknown repository type |
| weight keys in a weighted routing response | `HashMap` order | sorted | same call as nested settings key order, and for the same reasons. It does **not** extend to the two error messages that embed a rendered weights map: those are strings a client may compare, so hash order is reproduced there deliberately |
| discovered awareness values on a weighted routing `PUT` | rejects a weight set that leaves any value a routing node actually carries without a weight | not checked; the *forced* half of the same rule is | the value set comes from the live node list, which this service cannot read and should not cache — node attributes change as nodes join and leave, so a generated file would go stale while claiming authority, and the failure mode is rejecting a legitimate update. `cluster.routing.allocation.awareness.force.<attribute>.values` **is** a setting this service owns, so that half is enforced. Becomes a store read once the management plane projects live node state — which it now does, at `tenant/<t>/status/awareness`. That document supersedes **both** settings rather than only closing this gap: its values are the ones live nodes carry, and it is filtered by the pool's declared awareness attributes, so the name check moves there too. Settled since: the tenant's setting is read by no allocator, so the pool's declaration wins because it is the only one with an effect. One decision remains — what an absent document means, given the rejection and the read must land together. See [`docs/tenants-and-operators.md`](docs/tenants-and-operators.md) and `docs/proposed-awareness-attribute-narrowing.md` |
| awareness attributes set only in `opensearch.yml` | resolved from node settings too | invisible; weighted routing then rejects every attribute | this service knows which settings *exist*, not what a node has them set to. So weighted routing requires the attributes to have been set through this service. Accepting any attribute name instead would trade a visible rejection for weights that are silently never applied. **This divergence is on its way out rather than being fixed**: the attributes are the operator's to declare, not a tenant's to set here at all, and the allow-list moves to `tenant/<t>/status/awareness` — which is derived from the nodes, so a `opensearch.yml`-configured deployment stops being invisible. See [`docs/tenants-and-operators.md`](docs/tenants-and-operators.md) |

Two notes on scope rather than behaviour:

- **Placeholder names bind per-route**, where OpenSearch's trie stores them per-node — so
  `indices.rollover` sees `alias` here and `index` there. Never visible on the wire; it matters only
  to someone porting handler code. See [Design notes](#design-notes).
- **Superseded `_opendistro` routes are excluded** from the generated table. Currently a no-op for this
  surface: only `nodes.hot_threads` and `security.cache` carry those markers and both namespaces are
  out of scope. Listed so it is not mistaken for a divergence if that changes.

### How these get found

Every entry above was found by diffing against a live cluster, not by reading the spec — the spec
declares none of them. Several near-misses argue for keeping that habit:

- The 400 no-handler message omits the query string while the 405 message includes it. A request
  without query parameters makes the two look identical, which is how it was missed first time.
- `filter_path` treats arrays as transparent, so `*.index` matches nothing where it looks like it
  should match everything.
- `source` without `source_content_type` is a 500, not the 400 a malformed request would get.
- Deleting a nonexistent pipeline is a 200 on a pristine cluster and a 404 once any pipeline has ever
  existed — the trigger is metadata presence, not the current count.

## Verified against a live cluster

Confirmed against OpenSearch 3.9.0-SNAPSHOT. Bodies marked *byte-exact* were diffed
character for character against the server's response.

- **`GET /_ingest/processor/grok` has two orderings and the unsorted one is the default.** The parameter
  is `s`, one letter, and the spec documents it nowhere: the default answers `Grok.BUILTIN_PATTERNS`, a
  `LinkedHashMap` in pattern-file load order starting at `S3_REQUEST_LINE`, while `?s=true` answers a
  `TreeMap` starting at `BACULA_CAPACITY`. So the capabilities file stores **load order** — sorting is
  recoverable from it and it is not recoverable from sorted. A client passing `sort=true` gets the
  unsorted list, here and on a real cluster.
- **`GET /_script_context` has three orderings and none of them is a sort.** Contexts are sorted by name
  upstream, so the outer level looks alphabetical and is not being sorted by us. Within a context,
  `ScriptContextInfo.toXContent` emits `execute` **first** and then the getters sorted among themselves,
  so a getter whose name precedes the word "execute" follows it on the wire. Parameters are the signature
  — `moving-function` takes `(params, values)` and `similarity` takes `(weight, query, field, term, doc)`.
- **A parameter's fields are `type` then `name`**, the reverse of how the rest of this API reads and of
  how the capabilities file spells it. `ScriptContextInfo.ParameterInfo.toXContent` emits `type` first.
- **Ingestion pause and resume are verified end to end, across all three planes.** The recipe, worth keeping
  because it is the first test to exercise the whole system rather than one service: create a pull-based
  index through the control plane with an `ingestion_source` pointing at a file stream; write documents into
  the stream and confirm the data plane indexed them; `POST /{index}/ingestion/_pause` on the control plane;
  rewrite every document in the stream; `POST /{index}/ingestion/_resume` with
  `{"reset_settings":[{"shard":0,"mode":"offset","value":"2"}]}`. Only the document at offset 2 changes,
  because 0 and 1 are not reprocessed. So the pause flag reaches the data plane through the index document
  and the reset reaches it through the task record, both via the supervisor.
- **`mode` is accepted in any case and stored upper case.** Upstream parses it as
  `ResetMode.valueOf(text.toUpperCase(...))`, so both work, and normalising here means the supervisor and the
  data plane do not each need their own defensive upper-casing.
- **A malformed `reset_settings` entry has three distinct errors, not one.** `Invalid value for 'mode': x`,
  `Unexpected field in ResetSettings: x`, and `Missing required fields in ResetSettings: shard=0, mode=OFFSET,
  value=null` — note the last renders the *parsed* mode, so a lower-case `offset` appears upper-cased in its
  own error message. All from `ResetSettings.fromXContent`, which means
  `ResumeIngestionRequest.validate`'s single "ResetSettings is missing either shard, mode or value" is
  **unreachable over REST** — the parser rejects the same conditions first. Same shape as weighted routing's
  "Weight is not a number".
- **Controlling pull-based ingestion is metadata; performing it is not.** `ingestion` was excluded wholesale
  as "data plane", which was right about consuming from a stream and wrong about deciding whether a shard
  should be consuming at all. `pause` and `resume` are now in scope; `get_state` is not — every field it
  returns is per-shard poller runtime (`poller_state`, `poller_paused`, `batch_start_pointer`,
  `error_policy`, `is_primary`, the holding node) and the response pages over shards, so it is blocked on the
  same data plane projection as `cluster.health`.
- **The pause flag is a first-class index metadata field, not a setting.** Upstream it is
  `IndexMetadata.builder(...).ingestionStatus(...)` under the key `ingestion_status`, holding a single
  boolean `is_paused` that defaults to false. The obvious guess is `index.ingestion.paused` in the settings
  block, where a supervisor would find nothing.
- **Consumer-pointer resets are instructions, not state.** `_resume` optionally carries `reset_settings`,
  per-shard commands to move the consumer to an offset or timestamp first. They ride on the task record
  rather than the index document: recording them as state would mean replaying them on every reconciliation
  pass, or having the supervisor delete them afterwards — and the supervisor writing into this service's key
  space is what the plane boundary forbids.
- **A data stream cannot exist without a composable template that has a `data_stream` block.** Two
  separate errors, both `illegal_argument_exception`: no matching template at all, and a matching
  template that is not a data stream template. The template supplies the timestamp field name, so its
  absence is a rejection rather than a set of defaults.
- **Templates are matched against the stream name, not the backing index name.** `.ds-logs-000001`
  matches `logs*` nowhere, yet it gets that template's settings. Verified by giving a template three
  shards and reading them back off the backing index — byte-exact. Get this backwards and it fails
  silently, with the backing index quietly taking built-in defaults.
- **`create_data_stream` is not an upsert**, unlike templates and pipelines: a second `PUT` is
  `400 resource_already_exists_exception`, `data_stream [logs] already exists`. The existence check runs
  *before* name validation.
- **Data stream names are the index rules plus one, rewrapped.** The index reasons are reused verbatim
  under a `data_stream [x]` prefix and reported as `illegal_argument_exception` rather than the
  `invalid_index_name_exception` the index path gives. The extra rule: a data stream **may not start with
  a dot**, where an index may. It is checked after the shared rules, so `..` reports
  `must not be '.' or '..'` and `.logs` reports `must not start with '.'`.
- **`delete_data_stream` never reports a miss.** Every name is a glob matched against the streams that
  exist, with no not-found check anywhere — `resolveDataStreams` is a bare `Regex.simpleMatch` loop. A
  concrete name that does not exist is `200`, and so is a wildcard matching nothing. Unlike pipelines and
  repositories this is *not* the metadata-presence quirk; it is unconditional. It also deletes the
  backing indices, which is the one place an index disappears without `indices.delete` being called.
- **A backing index inherits its template's aliases**, exactly as an ordinary index does, and carries
  `index.hidden: true` on top of whatever the template composed.
- **Weighted routing is one document, not one per attribute.** The API reads as though
  `{attribute}` identifies a resource; it does not. A `PUT` for `rack` *replaces* weights
  previously set for `zone`, and a `GET` validates its path attribute and then ignores it — with
  weights stored for `zone`, `GET /_cluster/routing/awareness/rack/weights` returns them. Only
  `DELETE` compares the attribute against the stored one, and 404s when they differ. All three
  reproduced deliberately: the single-document shape is what the version counter counts.
- **The `_version` type differs between `PUT` and `DELETE`.** A number on `PUT`, either on
  `DELETE`. Not a quirk to smooth over — `PUT` parses its body with `mapOrdered` and `DELETE` with
  `mapStrings`, which coerces every scalar to a string first, so the asymmetry falls out of the two
  REST handlers. A string version on `PUT` is a parse error naming the *attribute*; a numeric one on
  `DELETE` is accepted.
- **Weights are strings on the wire and doubles underneath.** `"1"` comes back as `"1.0"`. A JSON
  number is a parse error whose message names the *previously* parsed weight — so a first bad weight
  reports `[null]`, because the server's parser keeps that variable outside its loop. Reproduced,
  oddity included.
- **A successful write stores `sent + 1`**, so a first write sends `-1` and lands at `0`. A `PUT`
  whose weights match what is stored is acknowledged *without* bumping the version. `DELETE` empties
  the document rather than removing it and bumps the version doing so — including when the weights
  were already empty, so it is not idempotent in the version.
- **`GET` on a cluster that has never had weights is `200 {}`**, an entirely empty object, where
  after a delete it is `{"weights":{},"_version":N,"discovered_cluster_manager":true}`. Another
  instance of a pristine cluster hiding behaviour: once weights have ever been set the metadata
  custom never goes away, so that branch cannot be re-entered without wiping the data directory.
- **`DELETE` does not validate the awareness attribute** where `GET` and `PUT` do — an unconfigured
  name gives `404`, not the `400` the others give. Nor does it need one: with nothing stored, the
  version compared against is `-1`, so a bodiless `DELETE` conflicts `-2` against `-1` rather than
  reporting nothing to delete.
- **Check order on `PUT`**, which decides which of several applicable errors a client sees: parse →
  `validate()` → awareness attribute → forced awareness values → version conflict. A malformed body
  outranks an invalid attribute; a stale version is reported last of all.
- **One condition, two messages, again.** The zero-weight majority check runs twice with different
  wording — `There are too many attribute values [...]` from the request's own `validate()`, and
  `There are too many discovered attribute values [...]` from the cluster-state task. Both embed the
  weights map rendered by `HashMap.toString()`, e.g. `{a=0.0, b=0.0, c=1.0}`.
- **A non-numeric weight escapes as a raw `number_format_exception`**, reason `For input string:
  "abc"` — the JDK's own message, because the server lets `Double.parseDouble` throw out of its
  parser rather than catching it into a validation error. Its `Weight is not a number` branch is
  therefore unreachable over REST.
- **The weighted routing version-conflict reason ends in a trailing space.** `requested version is 7
  but cluster weighted routing metadata is at a different version 17 ` — the server's format string is
  two concatenated literals and the second ends in one. Byte-exact, including that space, which needed
  reading outside the sandbox: the proxy truncates the response six bytes short of the message's last
  character.
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
- **Settings normalisation on every template surface.** `GET /_template/x`, the same with
  `flat_settings=true`, and `POST /_index_template/_simulate_index/x` are **byte-exact**; the composable
  and component template GETs match to the limit of what could be captured. All eight were diffed
  request for request against 3.9.0 after the `Settings` type landed. See [Settings](#settings).
- **Legacy template application.** The two kinds are alternatives, not layers, which is the part worth
  knowing: **if any composable template matches, the legacy ones contribute nothing at all** — verified
  by watching an index fall back to the built-in `number_of_replicas: 1` rather than a matching legacy
  template's `5`, losing its mappings and aliases with it. Where composable templates are *selected* by
  priority, legacy templates are *merged* — all matching ones, in ascending `order`, later winning; that
  is what `order` is for, and why overlapping patterns are rejected for one kind and legal for the
  other. Ties on `order` break by name. Eight requests diffed against 3.9.0, all agreeing.
- **`get_script_languages`.** Verified against 3.9.0 with a `capabilities.yaml` generated by
  `tools/dump-capabilities.sh` from that same cluster: **byte-exact for the whole 324-byte body** the
  sandbox proxy allowed through, covering `types_allowed`, the array-of-`{language, contexts}` shape, the
  field order within each entry, the language ordering and the context lists. Our full response is 664
  bytes; the untested remainder is more of the same pass-through data.
- **Stored scripts.** Sixteen requests diffed against 3.9.0 — **all byte-exact**, the cleanest run so
  far, and two of the rules would have been guessed wrong:
  - **`lang` is checked before `source`**, so `{"script":{}}` answers `must specify lang`. The reverse
    order is the intuitive one and would misreport the commonest malformed body.
  - **an empty body object stores a `mustache` script with an empty source** and answers 200. Not a
    guess — `StoredScriptSource.parse` returns exactly that for an immediately-closing object, alongside
    a deprecation warning that "empty templates should no longer be used". Reproduced because it is
    observable and cheap; it should go when upstream removes it.
  - missing reports two ways again: `GET` answers an ordinary `{"_id":"x","found":false}` with **no error
    document**, while `DELETE` answers `resource_not_found_exception`.
- **Snapshot repository registration**, verified in both cluster states — 11 requests against a cluster
  that has never had a repository, and a **17-step stateful sequence** against one with `path.repo`
  configured so real repositories could be created. All agree; 25 of the 28 byte-exact, the rest
  prefix-matched where the body listed two repositories.
  - **`GET /_snapshot/foo*` is a 404 on a pristine cluster and a 200 on one that has ever had a
    repository.** `TransportGetRepositoriesAction` takes an early branch when `RepositoriesMetadata` is
    absent, reporting the first token verbatim; the normal path lets a pattern matching nothing
    contribute nothing. Confirmed directly: with **zero** repositories but metadata once present, the
    same request answers `200 {}`. So the trigger is presence of metadata, not the count — the second
    instance of this artifact after `IngestMetadata`, reproduced the same way with a marker key. It was
    read from the source first, because probing an empty cluster only ever shows one side, and a first
    implementation got it wrong in both directions.
  - `DELETE` follows different rules again — a single glob rather than a comma list, with `*` exempt from
    the 404 where `foo*` is not, exactly as with deleting an ingest pipeline.
  - repository settings **round-trip nested and stringified**, and `uuid` is **not** in a GET response
    even though the spec's schema allows it.
- **`cluster.pending_tasks`** answers `{"tasks":[]}` byte-exact on an idle control plane. The non-empty
  shape could **not** be verified against a live cluster and is not claimed to be: a pending task exists
  only while a cluster-state update is queued, so there is no way to hold one still long enough to
  observe. Field names and their order come from `PendingClusterTasksResponse.toXContent`, which is
  definitive for both; what is unverified is how a real cluster's *values* would look beside ours, and
  three of those fields we cannot answer the same way at all — see `PendingTasksHandler`.
- **`indices.close` / `open`, and the `put_settings` branch they unlock.** A 15-step stateful sequence
  against 3.9.0 — zero divergences. Implemented for one reason: **a static setting may be changed on a
  closed index and not on an open one**, so until close existed there was a branch of `put_settings` no
  request could reach. What the probing turned up:
  - **`Final` and "merely static" are different things**, and collapsing them is wrong in a way that
    matters. `index.codec` carries neither `Dynamic` nor `Final` and is accepted on a closed index;
    `index.number_of_shards` is `Final` and is refused even then. Of the 221 index settings, 142 are
    dynamic, **45 are static** and 34 are final — so treating "not dynamic" as final would refuse 45
    settings a real cluster allows.
  - **the same setting reports differently depending on state.** While *any* target index is open, a real
    cluster pre-filters everything non-dynamic before reaching the code that tells static from final, so
    both answer `Can't update non dynamic settings [[...]] for open indices [[...]]`. Once every target
    is closed, only final is refused, as `final my-index setting [index.number_of_shards], not
    updateable`. One open index among closed ones is enough to trigger the first, and the message names
    only the open ones.
  - **wildcard expansion is asymmetric**: close expands to *open* indices, open expands to *closed* ones,
    so each reaches exactly what it would change. From `CloseIndexRequest`/`OpenIndexRequest`'s own
    defaults.
  - close's `indices` block lists **only what this request actually closed** — closing an already-closed
    index answers `"indices":{}` with `shards_acknowledged: false` — and open has no `indices` block at
    all. Closing writes `index.verified_before_close`; reopening **removes** it rather than setting it
    false.
- **Settings validation on `indices.create` and the template puts.** Fourteen requests diffed against
  3.9.0 — zero divergences. The interesting part is that **one condition has three different messages**
  depending on which service reaches it first, and all three are now reproduced:
  | supplying a private setting to | type | message |
  |---|---|---|
  | `put_settings` | `settings_exception` | `can not update private setting [index.uuid]; this setting is managed by OpenSearch` |
  | `create` | `validation_exception` | `Validation Failed: 1: private index setting [index.uuid] can not be set explicitly;` |
  | any template put | `invalid_index_template_exception` | `index_template [t] invalid, cause [Validation Failed: 1: ...;]` |

  Two further details that were wrong on the first attempt. An **unknown** setting on a template put is
  *not* wrapped — it answers a bare `settings_exception` where a **private** one is wrapped, for all
  three kinds. And `ValidationException` renders multiple errors with **no space between them**
  (`explicitly;2: private...`), which is easy to add by accident.

  Static settings are permitted on create and on template puts, because that is where they are set;
  only `put_settings` rejects them.
- **`cluster.get_settings` / `put_settings`.** A **14-step stateful sequence** replayed against 3.9.0 —
  set persistent, set another, read both, set transient, `flat_settings` on read and on write, a nested
  key, four rejections, a removal, read again. Zero divergences, nine of the fourteen byte-exact and the
  rest prefix-matched to the truncation limit. Because it is a sequence rather than independent cases,
  the accumulated state had to agree at every read, not just each response shape. Two findings:
  - **the PUT response echoes what the request assigned, not the cluster's settings.** With
    `cluster.max_shards_per_node` already set, a request setting `cluster.blocks.read_only` answers only
    the latter; a request that *removes* a setting answers `{}` for that scope, having assigned nothing.
  - **a body naming neither scope is "no settings to update"**, not an unknown-scope error:
    `{"bogus":{...}}` and `{}` both give `action_request_validation_exception`,
    `Validation Failed: 1: no settings to update;`.
- **`indices.put_settings`.** Eleven updates diffed against 3.9.0 with the real registry loaded —
  **zero status divergences and zero message divergences**, byte-exact wherever the live body was short
  enough to capture whole. Three findings worth keeping:
  - a **null removes** the setting rather than storing one, so it reverts to its default; and
    `preserve_existing=true` skips a key the index already has rather than failing on it.
  - the static-setting message doubles its brackets **for two different reasons** —
    `Can't update non dynamic settings [[index.number_of_shards]] for open indices [[my-index/uuid]]`.
    The settings list gets its brackets from the format string wrapping a `List.toString()`; the index
    list gets them from `Index.toString()` already rendering as `[name/uuid]` inside a list.
  - the `did you mean` suffix is reproduced, and getting it right needed Lucene's exact normalisation:
    **the edit distance is divided by the longer of the two strings**. A first cut divided by the
    shorter, which scores every candidate too low and silently truncates the suggestion list — caught
    only because a live cluster suggested two settings where this suggested one.
- **`indices.put_mapping`.** Fourteen updates diffed against 3.9.0 — **zero status divergences**, and
  ten of the fourteen messages byte-exact including the two-line parameter-conflict wording with its
  literal newline and tab. The four that differ are the `from [x]` clause described above. The
  updateability rules are not inferred from those probes: they are transcribed from OpenSearch's own
  `Parameter` declarations, where 55 of 87 sites are immutable. What the probes confirmed is that the
  transcription is right — `ignore_above`, `meta`, `search_analyzer`, `eager_global_ordinals`, adding a
  multi-field and adding an object property are all accepted on both sides, while `index`,
  `doc_values`, `store`, `similarity` and any type change are rejected on both.
- **The `indices` read set.** `get`, `get_mapping`, `get_field_mapping` and `resolve_index` were diffed
  request for request against 3.9.0 alongside `get_settings` — 22 requests, and every value agrees. The
  only differences anywhere were key order (the mapping-order divergence above) and the two volatile
  settings, `index.uuid` and `index.creation_date`. Four rules that are easy to assume wrongly:
  - **`indices.get` always emits all three blocks**, empty as `{}` — the opposite of `get_settings`
    with a name filter, where an index with nothing to report is dropped. The two rules look alike.
  - `get_field_mapping` keys each match by its **full dotted name** but nests the mapping under the
    field's **leaf** name: `{"user.name": {"full_name": "user.name", "mapping": {"name": {...}}}}`.
    Object fields match nothing; multi-fields under `fields` are leaves in their own right.
  - **`resolve_index` does not resolve.** An alias stays under `aliases` rather than flattening to its
    indices, `_all` is a literal name that matches nothing, and nothing is ever an error — an unmatched
    name is a 200 with three empty arrays. It is the one index expression that does not go through the
    resolver, and the reasons are on `ResolveIndexHandler`.
  - an index with no aliases **omits** the `aliases` key in `resolve_index`, while `attributes` is
    always present and reports `["hidden", "open"]` in that order.
- **`indices.get_settings`.** Thirteen requests diffed request for request against 3.9.0 — the name
  filter exact, wildcarded and comma-separated, the unprefixed form, a filter matching nothing,
  `flat_settings`, `provided_name`, `replication.type`, a missing index, a wildcard matching nothing,
  `allow_no_indices=false` and `ignore_unavailable=true` — and all thirteen agree. The only difference
  anywhere was key order within a nested settings object, which is the sorted-versus-`HashMap`
  divergence above; the values were identical. Three rules that are easy to get wrong and were verified
  rather than assumed:
  - the name filter matches the **full dotted key** and gets no implicit `index.` prefix, so
    `/_settings/number_of_shards` answers a bare `{}` while `/_settings/index.number_of_shards` works
  - an index whose filtered settings come out empty is **dropped from the response**, not rendered as
    `"settings": {}`
  - `index.replication.type` is a real index setting, not a server-supplied one: it is settable on
    create, defaults to `DOCUMENT`, and is **case-sensitive** — a real cluster rejects `segment` with
    `Could not parse ReplicationStrategy for [segment]` and accepts `SEGMENT`. That value check is not
    reproduced here, since settings are stored unvalidated.

## The global query parameters

The spec attaches five parameters to every operation, and codegen collects them into
`GeneratedRoutes.GLOBAL_QUERY_PARAMS`. They are handled centrally — in `Json.response` and
`Router.dispatch` — so no handler thinks about them and every new handler inherits them.

| parameter | status |
|---|---|
| `pretty` | implemented; two-space indent matching the server |
| `filter_path` | implemented; see below |
| `source` + `source_content_type` | implemented; body supplied in the query string |
| `error_trace` | implemented for escaped exceptions; deliberately not for checked failures — see below |
| `human` | **deliberately not supported** — see below |

### filter_path

Semantics verified against a live cluster and diffed filter-by-filter: dot-separated paths keep their
nesting rather than flattening, `*` matches one segment and `**` matches zero or more, `-` excludes,
an exclusion prunes inside an inclusion, and nothing matching gives `{}`.

Two details that are easy to get wrong, and were:

- **Arrays are transparent.** An array index is not a path segment, so `filter_path=index` over
  `[{"index":"a","health":"green"}]` gives `[{"index":"a"}]` and `*.index` matches *nothing*.
- **It does not apply to error responses.** A real cluster ignores `filter_path` when rendering an
  exception, so `GET /nope?filter_path=status` returns the whole error body. Reproduced by filtering
  only on the success path.

### error_trace, and what it deliberately omits

A real cluster can trace any error, because in OpenSearch every error *is* a thrown exception. Here
most are not: `indexNotFound` and the validation failures build a response directly, because a
rejected index name is an expected outcome rather than an exceptional one.

So `error_trace=true` yields a trace when an exception genuinely escaped a handler, and nothing when
the failure was explicitly checked for. **This is the intended scope, not a gap awaiting a refactor.**
Like `human`, `error_trace` is a debugging aid for a human reading a response rather than a signal a
production client acts on — and where it actually helps is precisely the unforeseen case, which is the
one that does carry a trace. Throwing an exception per validation failure would put that cost on an
ordinary path and produce a trace pointing at our own validation code rather than at anything the
caller could act on; the `reason` already says exactly what was wrong.

### Two parameters aimed at humans, not programs

`human` and `error_trace` both exist for someone reading a response. Both are scoped by judgement
rather than reproduced exhaustively, which is why the divergences above and below are deliberate.

### Why human is not supported

Not an omission. Reformatting durations and byte sizes for human eyes is a client-side presentation
concern; a response format should stay machine-parseable, and a parameter that changes value
representations mid-API works against that. The view is shared within the OpenSearch community and the
parameter is a candidate for deprecation, so declining it from the start leaves this service ahead
rather than behind.

It is **accepted and ignored rather than rejected**, because the control plane should be robust to
callers that send it: `human=true` gets a correct machine-readable response, not an error.

### One quirk worth knowing

`source` without `source_content_type` is a **500** `illegal_state_exception`, not the 400 a malformed
request would normally get. Verified, and reproduced.

## Still unverified

- Whether the 404-on-no-match behaviour of `GET /_ingest/pipeline` differs for an
  explicit comma-separated list where *some* IDs match and others do not (partial
  match is assumed to be a 200 here).
- Everything about the operation groups that are still stubbed. With a full capabilities file that is
  **17**, and there is no easy CRUD left among them: eight are snapshot data or administration
  operations that belong to another plane, one is the deferred `rollover`, three are decommission
  awareness, and the remaining five — `cluster.health`, `cluster.state`, `indices.get_data_stream`,
  `indices.add_block`, `cluster.remote_info` — each have their reason recorded here or in
  [`docs/where-the-hard-parts-are.md`](docs/where-the-hard-parts-are.md). (The snapshot count read
  *seven* before and the total never added up; it is eight, and the arithmetic now closes.)
- **`indices.get_data_stream` is stubbed for a specific reason, not just because it is next.** Its
  `status` field is a `HealthStatus` over the backing indices and the spec marks it **required**, so
  unlike `pending_tasks`' prose durations it cannot simply be omitted and leave a valid response. Health
  is shard state, which is the same dependency that keeps `cluster.health` stubbed; when the management
  plane projects it into the shared store this becomes an ordinary store read. `create_data_stream` and
  `delete_data_stream` need none of it and are implemented.
- **`indices.get_data_stream`'s other fields are unverified against ours** for the same reason — there is
  nothing to diff yet. The shape a real cluster returns *was* captured while probing:
  `{"data_streams":[{"name":..,"timestamp_field":{"name":".."},"indices":[{"index_name":..,"index_uuid":..}],"generation":N,"status":"GREEN","template":".."}]}`,
  sorted by name, with a concrete miss answering `404 index_not_found_exception` — note **index**, not
  data stream — and a wildcard miss answering `200 {"data_streams":[]}`.
- **`indices.get_alias` excludes hidden indices where a real cluster includes them.** `AliasLookup`
  defaults to `IndicesOptions.lenient()`, which expands wildcards to open indices only, but
  `GetAliasesRequest` defaults to `strictExpandHidden()` — open, closed **and** hidden, with
  `ignore_unavailable` false rather than true. Nothing exposed this until data streams created the first
  hidden index: a template alias on a backing index is written correctly and then not reported.
  Pre-existing, and its own fix — the three flags want probing against closed and hidden indices
  together rather than being flipped on the strength of the source alone.
- **Data streams stay at generation 1.** `rollover` is what advances the generation and add a backing
  index, and it is deferred with the rest of the resize family. The stored `generation` counter exists so
  that rollover has something to increment.
- **`shards_acknowledged` deliberately diverges.** A real cluster returns `true`; this returns `false`
  unless `wait_for_active_shards=0`, because it returns before any shard exists and will not claim
  otherwise. `false` is not an error signal in OpenSearch either — it is what a real cluster returns
  when the wait times out. Everything else about create, delete, and their error bodies is
  byte-identical to OpenSearch 3.9.0.
- **Mapping merge has no conflict detection.** Two templates disagreeing about a field's type produce a
  last-writer-wins mapping rather than the rejection a real cluster raises. Belongs with
  `indices.put_mapping`.
- **Settings existence and permission are validated; values are not.** With a `--settings-registry`,
  every write path rejects an unknown or private setting as a real cluster does. What is not checked is
  whether a *value* is well-formed, so `index.replication.type: "segment"` is accepted here and rejected
  there (which insists on `SEGMENT`), and an incomplete analyzer definition is accepted here and fails
  with `Failed to build analyzers` there. Values stay the data plane's authority — the same boundary
  that keeps `include_defaults` out.
- **`index.uuid`, `index.creation_date` and `index.provided_name` are not in a created index's
  settings.** A real cluster writes them there; here they are top-level metadata fields, and
  `indices.get_settings` renders them back in on the way out. Same information, held once rather than
  twice — but anything else reading stored metadata directly has to know that.
- `filter_path` behaviour over deeply nested arrays-of-arrays, and interactions between `**` and
  exclusions, are implemented from the documented model rather than exhaustively diffed.
- **Processor validation is opt-in, and its data comes from outside.** With `--capabilities` the
  control plane rejects unsupported processor types up front; without it, anything is accepted. See
  [Data plane capabilities](#data-plane-capabilities). The same question is still open for index
  settings and analyzers, which are equally plugin-dependent and equally unvalidated.
- The generated capabilities file is a point-in-time snapshot. Nothing detects that the data plane has
  changed underneath it, so a stale file rejects newly-available processors. The store-published
  version would not have this problem.
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

[`docs/where-the-hard-parts-are.md`](docs/where-the-hard-parts-are.md) surveys the stubbed
operation groups by what makes them difficult rather than by namespace. The short version:

- **Aliases are their own keyed resource** (`aliases/<name>`), not fields inside index metadata. The
  canonical swap is therefore a single-key compare-and-swap: no multi-key transaction support is
  needed, and object-store backends stay viable. Note the win is narrower than first assumed — alias
  reads are index-keyed even when queried by alias name, so index→aliases remains a scan.
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
cd /path/to/opensearch-api-specification && npm run merge
cp build/opensearch-openapi.yaml /path/to/cluster-etcd/control-plane/spec/opensearch-openapi.yaml
cd /path/to/cluster-etcd/control-plane && ../gradlew :server:generateRoutes
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
  metadata/               Settings -- the canonical settings representation
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
