# Working notes for this environment

Not about the design — `REQUIREMENTS.md` and `NODE_CONTRACT.md` have that. This is the set of things that
cost hours to discover, written down so they cost minutes next time. The supervisor repository next door has
its own copy for the parts that are about the sandbox rather than about OpenSearch; those are not repeated
here beyond a summary.

## What OpenSearch 3.9 actually does

### Index settings are strictly validated on the apply path

The important one, because the intuition is wrong in the dangerous direction. It is tempting to believe an
index setting the server does not know is ignored — that is the tolerant behaviour that lets an index
survive being moved to a cluster missing some plugin. **It does not apply to a state being applied.**

`IndicesService.createIndexService` calls
`indexScopedSettings.validate(indexMetadata.getSettings(), true, true, true)` before building the
`IndexModule`, and `AbstractScopedSettings.validate` resolves each key with `getRaw(key)`; a null throws
`SettingsException: unknown setting [...] please check that any required plugins are installed`. So an
unregistered setting in a fabricated `ClusterState` fails **every index on the node**, at index creation,
with a stack trace pointing at the server.

The tolerant behaviour is real and lives in two other places, neither on this path:

- `MetadataIndexUpgradeService` and `ClusterStateUpdaters` call
  `AbstractScopedSettings.archiveUnknownOrInvalidSettings`, on metadata read from disk during gateway
  recovery or a version upgrade.
- And note what archiving does: it **renames** the key to `archived.*`. Even where it applies it preserves
  the bytes and loses the meaning, so it is no use to anything that has to read the value back.

Consequences, both now in the code:

- Every setting this plugin puts into a fabricated index **must** come back from `Plugin.getSettings()`.
  `Node` collects that list, `SettingsModule` filters it by `hasIndexScope()`, and the survivors become the
  registry the call above validates against.
- `Property.InternalIndex` is the right way to say "this is mine, not an operator's". Internal and private
  settings are only rejected when `validateInternalOrPrivateIndex` is true, which the comment in
  `AbstractScopedSettings` says is "only if this call is coming via the update settings API" — so the apply
  path accepts them and a user's attempt to set one is refused with *"can not update internal setting"*.

**And the control plane already refuses them, which closes the obvious hole.** A user could otherwise
create an index whose settings include `index.clusterless.tenant`; the control plane would store it and this
plugin would overwrite it, which is survivable but lets a user put a misleading tenant into a document an
operator reads during an incident. The settings registry the control plane was built around handles it, and
it needs nothing from this repository beyond the `InternalIndex` property:

- `GET /_cluster/settings_registry` renders every setting in the node's **live** `IndexScopedSettings` —
  which is the registry `SettingsModule` built from the built-ins *plus* `Plugin.getSettings()` — and emits
  each `Setting.Property` by `name()`. The endpoint's own javadoc gives the reason: so a consumer need not
  hardcode a list that goes stale "on the next upgrade or the next plugin install".
- The control plane snapshots that into `settings_registry.json`, and `DeclaredSettings` maps the literal
  strings `InternalIndex` and `PrivateIndex` onto `Updateability.NOT_USER_SETTABLE`.
- `SettingsValidation.check` refuses those on `indices.create`, `put_settings` and the template puts —
  **unconditionally**, unlike `STATIC` and `FINAL`, which `indices.create` legitimately permits. That
  unconditional branch is what makes this work at creation time and not only on update.

There is precedent in the snapshot already: the five `index.searchable_snapshot.*` settings are declared
`InternalIndex` and land in the registry exactly this way.

Worth noticing that **both** possible states of that snapshot are safe, which is why this needs no
coordination. Regenerated from a node carrying this plugin, `index.clusterless.tenant` is present and
`NOT_USER_SETTABLE`; regenerated from a node without it, the key is simply unknown and rejected as such. The
two differ only in which error the user gets. The one case that permits it is a control plane running a
non-enforcing registry, which is that service's deliberate default when nothing is declared and not
something specific to these settings.

`ClusterlessIndexSettingsTests` pins the two ends of the coupling that are invisible from either side alone:
that each identity setting is in the registry the endpoint walks, and that its property is spelled the way
the control plane matches.

### Plugin callbacks do not all run after `createComponents`

Obvious once stated and not before: `createComponents` is the first callback handed a `ThreadPool`, an
`Environment` or a `ClusterService`, and some callbacks run **before** it. From `Node.<init>`:

| line | what | can it use `createComponents`' arguments? |
|---|---|---|
| 830 | `new ClusterModule(...)` → `getIndexNameCustomResolvers()` | **no** |
| 1212 | `createComponents(...)` | this is where they arrive |
| 1360 | `new ActionModule(...)` → `getActionFilters()` | yes |
| 1953 | `initRestHandlers(...)` → `getRestHandlers()` | yes |

`getSettings()` runs earlier still and is fine, because it returns static descriptions rather than using anything.

Getting this wrong cost the whole node: a resolver that captured `threadPool` at construction threw
`NullPointerException` out of a plugin callback during bootstrap, and OpenSearch does not start with a plugin that
cannot be asked for its resolvers. The fix is a late-bound field the resolver reads per request, which is safe here
only because the failure mode is benign — no tenant means no index expression is rewritten, and an unqualified name
then fails to resolve rather than resolving to another tenant's data.

**`ClusterlessPluginTests` pins it**, and it is worth keeping: the ordering is upstream's and can change under us,
and this is the one class of bug in this plugin that stops a node from existing at all rather than degrading it.

### `IndicesService` is still only reachable through Guice

Neither `createComponents` overload receives it — including the newer one that takes a
`PluginComponentRegistry` — and it is not in that registry either. So `cluster-etcd`'s `GuiceHolder`, a
lifecycle component whose only job is to capture the reference into a static field, remains the only way in.
Checked against 3.9; this answers `REQUIREMENTS.md` open question 4 with a no.

### XContent is built on Jackson 3, and Jackson 2 may also be present

`org.opensearch.common.xcontent` uses `tools.jackson.*` (Jackson 3.2.2 in 3.9), not
`com.fasterxml.jackson.*`. The symptom of getting this wrong is a clean compile followed by
`NoClassDefFoundError: tools/jackson/core/ObjectReadContext` from the first `XContentBuilder`.

Both live on this project's test classpath at once, under different package names, and do not shadow each
other: XContent needs Jackson 3, and the supervisor's codec — which the conformance suite reads these
documents with — needs Jackson 2. That is fine, and it is worth knowing before trying to reconcile them.

### A classpath that compiles is not a classpath that runs

`javac` needs what the signatures mention; static initialisers reach much further. `ClusterSettings`'
`<clinit>` alone pulls in the telemetry lib and Lucene's analysis package, so a classpath trimmed to what
compilation wanted failed at runtime inside a class this plugin only mentions in a test.
`tools/jbuild.sh` therefore takes `server` plus **every** built lib, and every Lucene artifact at the
version `../OpenSearch/gradle/libs.versions.toml` pins — several Lucene versions may be cached,
and picking the newest rather than the pinned one fails somewhere specific and confusing.

### `ShardRoutingState` has five values

`UNASSIGNED`, `INITIALIZING`, `STARTED`, `RELOCATING`, `SPLITTING` — the last being in-place shard split,
which `NODE_CONTRACT.md` predates. Mapped to `STARTED`, matching the server's own `active()`. Worth writing
the mapping as an exhaustive `switch` rather than a chain of `if`s: that is what turned a new enum constant
into a compile error instead of a silent mislabelling.

### Every store call has to be privileged, and the plugin's policy alone cannot help

The first attempt to run a node failed before it started:

```
SecurityException: Denied WRITE access to file: .../plugin/build/clusterless-store,
domain: ProtectionDomain (file:.../lib/opensearch-cli-3.9.0-SNAPSHOT.jar)
```

Note which jar is named. It is not this plugin's, and `opensearch-cli` has nothing to do with the store — it is
simply further up the stack, because `createComponents` runs from the bootstrap path. **That is the whole
lesson**, and adding grants to `plugin-security.policy` cannot fix it: a plugin's policy applies to the plugin's
own jars, and the check was failing on somebody else's.

OpenSearch 3.x enforces its policy with a java agent (`libs/agent-sm/agent`) rather than a SecurityManager.
`FileInterceptor` and `SocketChannelInterceptor` walk the stack and require **every** protection domain on it to
hold the permission. Since a store call is reached through the thread pool that scheduled the pass, the server's
own jars are on the stack — and the server has no permission to write to a directory an operator named in a
plugin setting, nor to open a socket to an etcd cluster it has never heard of.

The escape is `org.opensearch.secure_sm.AccessController.doPrivileged`, and it works because the agent's
extractor is written as `takeWhile(frame is not doPrivileged)`. Three things worth knowing about it:

- **It is the modern one.** `java.security.AccessController` still works — the extractor recognises both — but
  there are zero uses of it left in `server/src/main`, and it is deprecated for removal.
- **It needs no new dependency.** `server` has `api project(':libs:opensearch-secure-sm')`, which in turn has
  `api project(':libs:agent-sm:agent-policy')`, so any plugin compiling against the server already has it. Do
  *not* add it as an `implementation` dependency: `opensearch-agent-policy` is already in the distribution's
  `lib/`, and a bundled second copy is jar hell, which fails every node at startup.
- **Its implementation is `action.run()`.** It does nothing but put a recognisable frame on the stack. That also
  means a unit test cannot verify any of this: no agent is installed, so the block is transparent and the policy
  is never consulted. Only a running node checks it.

So `PrivilegedKeyValueStore` wraps **every** call, not just the ones that obviously touch a disk — reads are
checked too, and a network backend is checked the same way by the socket interceptor. It lives in this
repository rather than in `store-api`, because the store is also used by two ordinary JVM services with no
policy at all and should not learn about OpenSearch's security model to satisfy one of its three consumers.

### The store cannot tell a returning node that it held a shard

Found while choosing a recovery source, and it is a gap by construction rather than an oversight. Two decisions
in `NODE_CONTRACT.md` are individually right and together leave a hole:

- allocation ids are deliberately **not** in the shard inventory — measured at 37% of that document's size
  budget for a field the supervisor has no use for; and
- reading them back out of the store is explicitly listed as a thing not to copy from `cluster-etcd`, because
  "allocation ids are on the node's local disk; a store round-trip makes them lossable in a way they otherwise
  would not be".

So nothing an assignment can say distinguishes *"create this copy"* from *"you already have this copy, resume
it"*. Only the disk knows. That is what D2.5 is really asking for when it says a node must converge from an
empty disk and from a returning one "without special-casing either" — the answer is not to special-case, it is
to look, which is `LocalShardStore` and `NodeEnvironment.availableShardPaths`.

The consequence is asymmetric, which is why it gets a seam and a conservative default rather than an inline
check: answering "no data" when there is some recovers from empty and **discards the shard**, while answering
"there is data" when there is none fails the recovery visibly and recoverably. When in doubt, claim data.

### XContent reaches every dataformat, not just the one you write

`CompressedXContent` — and therefore `MappingMetadata`, and therefore anything that fabricates index
metadata — auto-detects the format of a byte stream, so it touches the CBOR, Smile and YAML dataformats even
for a plugin that only ever writes JSON. The symptom is `NoClassDefFoundError: tools/jackson/dataformat/cbor/...`
from a test that looks like it only builds a cluster state. `tools/jbuild.sh` carries all four.

### A cluster-manager action hangs for thirty seconds and then lies about why

Indexing a document into an index whose mapping does not cover it produced this, after a thirty-second wait:

```
{"type":"cluster_manager_not_discovered_exception","reason":null,"status":503}
```

Every part of that is unhelpful. The status says "retry later" for something that can never succeed; the reason is
null; and the actual cause — the write needed a mapping change — is nowhere in it. What happens is that the server
sends `indices:admin/mapping/auto_put` to the cluster manager and waits `indices.mapping.dynamic_timeout`.

`PutMappingRequest extends AcknowledgedRequest extends ClusterManagerNodeRequest`, which is the hook: an
`ActionFilter` can see the request type and answer without a manager. `ClusterManagerActionFilter` does what
`cluster-etcd` does — mark `ClusterManagerNodeReadRequest` local and let it through, refuse anything else that
needs a manager — with two additions. Refusals name the control plane, because in this design a rejected mutation
has somewhere specific to go. And `internal:` actions are skipped entirely: shard-started notifications travel
through `LocalShardStateAction`, and a filter that refused server-to-server traffic would break the shard
lifecycle this plugin exists to drive.

**This does not make dynamic mapping work, and it should not.** Mappings are desired state; they come from the
control plane. What changed is that the failure is immediate, a 400, and says which service to send the mapping to.

### Retained metadata makes local `cluster.health` report nonsense

Deleting an index leaves this node holding its metadata with an empty routing table, which is what stops the
server deleting the files. The cost shows up in `GET /_cluster/health`, answered from local state:

```
"status" : "red",
"active_primary_shards" : 0,
"unassigned_shards" : 0,
"active_shards_percent_as_number" : "NaN"
```

Red, with nothing unassigned, and a percentage that is not a number. Health is computing over an index state
OpenSearch itself never produces — metadata declaring one shard, a routing table containing none — so it has
nothing sensible to say.

Two things follow, and both were already on the list for other reasons. **Retained metadata has to be reaped**: the
node knows the tenant and name of every index it retains, because they are in the fabricated settings, so it can
check whether the index document is still in the store and drop the metadata once it has been gone for K passes —
the supervisor's `ORPHANED_COPY` discipline turned inward. And **health belongs to the supervisor's projections**,
not to local state: `tenant/<t>/status/cluster` is computed from the fleet, and would simply not mention an index
that no longer exists.

Until both exist, a node that has outlived a deleted index reports red. Worth knowing before it is mistaken for a
broken shard.

### The fabricated cluster name has to be given, not inherited

`GET /_cluster/state` reported `"cluster_name" : "opensearch"` while `GET /_cluster/health` reported
`"cluster_name" : "runTask"` on the same node. `opensearch` is `ClusterName.DEFAULT`, and the state's name was
being copied from the previous state — so whatever the first fabrication of a process was handed propagated for
ever, while health read its name from the settings.

Harmless in a single-node run and not worth leaving: a cluster name is one of the things a transport handshake and
a good deal of tooling check, and two answers from the same plugin disagreeing about which cluster they describe
is its own kind of bug.

### `createIndexMapperService` is expensive, side-effecting, and looks like a shard restart loop

The most misleading symptom so far. A node with one healthy index logged this once a second, for ever:

```
[o.o.p.PluginsService     ] PluginService:onIndexModule index:[default.my-index.c526cf4262/TBOf...]
[o.o.i.MergeSchedulerConfig] Initialized index default.my-index.c526cf4262 with maxMergeCount=9, ...
```

which reads unmistakably as an index being created over and over. Nothing was wrong with the shard. The lines come
from `IndicesService.createIndexMapperService`, which the mapping canonicalisation calls, and which

```java
final IndexSettings idxSettings = new IndexSettings(...);   // MergeSchedulerConfig logs here
final IndexModule indexModule = new IndexModule(...);
pluginsService.onIndexModule(indexModule);                  // PluginService logs here
```

does on the way to building a throwaway `MapperService` — inside a `synchronized` method that real index
operations also queue behind. So canonicalising on every pass is not merely noisy: it fires every installed
plugin's `onIndexModule` hook and serialises against index work, once per poll interval per index.

The fix is to skip it when it cannot matter. The store revision of the index document is carried in the fabricated
settings, so if the previously applied metadata came from the same revision, the document has not changed and
neither can its mapping — the applied `MappingMetadata` is reused verbatim. Canonicalisation then runs once per
revision, which is the frequency it was always for.

**The general lesson is the one from the log-spam bug, one level up.** The fabricator is a pure function called
speculatively on every pass, so anything expensive or observable inside it happens far more often than the code
suggests — and when the observable thing is somebody else's log line, the symptom points anywhere but here.

### A fabrication failure must be contained to one index

Building the whole cluster state is a single operation, so an exception anywhere in it aborts all of it: the node
applies nothing, holds nothing new, and retries the same failing fabrication on the next pass, for ever. What
that looked like from outside was a shard re-initialising once per second, with the real cause a hundred lines
earlier in the log.

The cause was a `NullPointerException` in the mapping merge — `IndexMetadata.mapping()` is null on an index
created with no mappings, which is exactly what `indices.create` with no body produces, and setting a mapping on
such an index then reached the merge path. `cluster-etcd`'s version of that method has the same latent bug; it was
copied faithfully enough to include it.

Both halves are now fixed, and the second one is the one worth keeping: a failure building one index costs that
index a pass and nothing else, with the previous state's version carried across untouched. That is D2.4 — *"one
unrecoverable shard must never stop a node applying the rest of its assignment"* — read as covering a shard this
code cannot *describe*, not only one that will not recover. Carrying the previous version forward is also the safe
direction for the data: a copy already serving keeps serving, and the index does not vanish from the metadata,
which is what would tell the server to delete its files.

### Do not log from inside the fabrication

`buildClusterState` runs on **every** pass — twice on a pass that applies, since the state is built once to
compare and once inside the applier — so an `INFO` line in it is a line per poll interval for as long as the
condition holds. Closing an index produced one per second saying the metadata was being kept. The fact belonged in
the report, which prints only when something changes.

Worth generalising: the fabricator is a pure function called speculatively, and anything with a side effect in it
happens more often than it looks like it does.

### `./gradlew run` deletes the data directory on restart

Corrected by testing: testclusters wipes it unless told otherwise, so a restarted node finds nothing and recovers
from an empty store — which is correct behaviour and proves nothing about the existing-store path.
`--preserve-data`, or `--data-dir=<path>` (always preserved), is how to test a returning node.

The existing-store path is nonetheless **confirmed**: closing an index and reopening it recovered with
`EXISTING_STORE`, which is `ExistingStoreRecoverySource` paired with a freshly generated allocation id — the one
piece of the fabrication with no precedent in `cluster-etcd` and no test that could prove it.

### A replica needs a remote store, because the alternative reads a routing table we do not have

`SegmentReplicationSourceFactory.get` is the whole reason M7 is about the remote store:

```java
if (shard.indexSettings().isAssignedOnRemoteNode()) return new RemoteStoreReplicationSource(shard);
else return new PrimaryShardReplicationSource(..., getPrimaryNode(shard.shardId()));
```

`getPrimaryNode` resolves the primary through `clusterService.state().routingTable().shardRoutingTable(shardId)
.primaryShard()` and then `state().nodes()`. Both come back empty on a node whose state contains only its own
copies and only itself. So the `else` branch is unreachable-by-design here until peer resolution exists, and a
replica built on it would start, report itself started, and never receive a segment. That is why the copy is
declined instead.

`isAssignedOnRemoteNode()` is computed from **node** settings — `node.attr.remote_store.*` — not from index
settings and not from cluster state. Useful: the repository is local operator configuration, so nothing about it
has to travel through the key layout.

### Nothing but a cluster manager registers the remote-store repository

`RepositoriesService` is a `ClusterStateApplier` that builds its repositories from
`Metadata.custom(RepositoriesMetadata.TYPE)`, with no cluster-manager gating — so putting the metadata in the
fabricated state is all it takes. And it is *necessary*: the only main-code callers of
`RemoteStoreNodeAttribute(node)` are in `JoinTaskExecutor`, the join path, and a clusterless node never joins.
`RemoteStoreNodeAttribute` already stamps `system_repository=true` into the metadata it derives, so what the plugin
puts in the state is what the server itself would have.

Ordering works out: `RepositoriesService` registers its applier in its constructor, before Guice builds
`IndicesClusterStateService`, and both are high priority — so on the state that first carries an index, the
repository is created before the shard that needs it.

`DiscoveryNode.isRemoteStoreNode()` requires a **cluster-state** repository as well as a segment one, and
`RemoteStoreNodeAttribute` throws on a node that names a segment repository without the translog and cluster-state
ones. All three can name the same repository; remote cluster state stays off, and there is no cluster state here to
publish anyway.

### `index.blocks.search_only` is a marker here, not a block

Two facts that only make sense together:

- `RoutingNodes` asserts `"Primary shard routing can't be null for non-search-only indices"` for any shard whose
  routing table has no primary, unless the index sets `index.blocks.search_only`. It is an `assert`, and
  `IndicesClusterStateService` builds `RoutingNodes` on every applied state, so with `-ea` — which testclusters
  uses — a search replica without the setting is fatal. `IndexRoutingTable.validate` has the same escape hatch,
  though it turns out to have **no callers in server main** at all, which is why a node holding only the primary of
  an index whose config says `number_of_replicas: 1` has never thrown.
- The setting's block, `INDEX_SEARCH_ONLY_BLOCK` (`ClusterBlockLevel.WRITE`, 403, "index scaled down"), is only ever
  derived from it by cluster-manager code: `MetadataCreateIndexService`, `RestoreService`,
  `RemoteStoreRestoreService`, the gateway's `ClusterStateUpdaters`, `LocalAllocateDangledIndices`. None of that
  runs here. A fabricated state sets no blocks, so the setting costs nothing.

Which is what makes a node holding a primary of shard 0 and a search replica of shard 1 of the same index work —
and that is the *natural* placement with two nodes and a two-shard index, not an edge case. **This is dependence on
the absence of a code path, so it is pinned by a test** rather than left to be rediscovered as 403s on writes.

`index.number_of_search_replicas` is a separate decision from the marker, for a separate reason:
`cluster.routing.search_replica.strict` defaults to **true**, and with it on `OperationRouting` injects
`Preference.SEARCH_REPLICA` into every search that named no preference, for any index whose metadata says it has
search replicas. On a node holding a primary that selects from an empty set — a healthy primary answering nothing.
So the count is 1 only when every copy here is search-only. A started search-only shard is added to
`IndexShardRoutingTable.activeShards` unconditionally, so the default iterator still reaches it when the count is 0.

### The remote store path must be derived, never stamped

`RemoteStoreUtils.determineRemoteStorePathStrategy` reads the index metadata's `REMOTE_STORE_CUSTOM_KEY` and falls
back to `PathType.FIXED` when there is none. In a real cluster one cluster manager stamps that custom data once and
every node reads the same copy. Here every node fabricates its own metadata independently, so anything stamped per
node is free to differ between a primary and its replica — and two copies of a shard disagreeing about where their
segments live shows up as a replica that reads an empty store, not as an error. So the fabrication sets **no**
remote-store custom data, and every node reaches `FIXED` by the same route. Choosing the older, flatter layout over
one that might disagree with itself.

The same reasoning is why `index.replication.type` is read from the index document rather than from a node-local
default: a default is the one input that could make two nodes disagree about the same index.

### Remote store: three predicates that are not the same predicate

Worth a table, because picking the wrong one produces a broken node rather than an error.

| predicate | true when | used for |
|---|---|---|
| `DiscoveryNode.isRemoteStoreNode()` | cluster-state **and** segment repositories | nothing this plugin should gate on |
| `RemoteStoreNodeAttribute.isRemoteDataAttributePresent(settings)` | segment **or** translog repository | `IndexSettings.assignedOnRemoteNode`, which chooses the segment replication source |
| `IndexSettings.isRemoteTranslogStoreEnabled()` | `index.remote_store.translog.repository` is set | which translog factory a primary gets, and therefore whether `REMOTE_STORE` recovery works |

`node.attr.remote_store.mode=segments_only` — added upstream for this architecture — makes
`RemoteStoreNodeAttribute` require the segment repository alone. Such a node is therefore **not** an
`isRemoteStoreNode()`, while `assignedOnRemoteNode` is still true for its shards. Gating on the first would leave
the server routing replicas to `RemoteStoreReplicationSource` with no repository registered: the one genuinely
broken state available here. `RemoteStore` keys on the segment repository, and `RemoteStoreTests` pins it.

In the default mode all three repositories are mandatory — a segment repository without a translog and a
cluster-state one throws `IllegalStateException` naming the missing attribute, at startup, which is the right place
to find out.

### `REMOTE_STORE` recovery fails on an empty repository, and that inverts the safe direction

The instinct is that recovering from a remote store which happens to be empty produces an empty shard, so
preferring it when unsure is the cautious choice. It is not.

`RemoteFsTranslog.download` takes an `else` branch when there is no remote translog metadata — "we don't have any
translog files uploaded to remote store" — and creates no `translog.ckp`. `StoreRecovery.recoverFromRemoteStore`
then finds the local store empty, so `listAll().length <= 1` holds, and `Checkpoint.read(location.resolve(
CHECKPOINT_FILE_NAME))` throws `NoSuchFileException`, wrapped as `IndexShardRecoveryException`. Since a failed copy
is handed a fresh routing entry on the next pass, a new index whose primary defaulted to `REMOTE_STORE` never
starts at all.

So both directions are unsafe and the answer must be *looked up*: `EMPTY_STORE` over data is silent loss,
`REMOTE_STORE` over nothing is an infinite loop. Where it cannot be looked up, decline the copy — see
`RemoteRecovery`. And `UNKNOWN` must never collapse into `EMPTY`: an unreachable repository and an empty one are
the same silence and opposite facts.

### D5.3 is satisfied by the server, not by us

A search replica placed in the same pass as its primary does not need to be made to wait. Its recovery goes
`recoverFromStore` → `internalRecoverFromStoreForSearchReplica` → `completeRecovery`, which creates an empty store
and *then* calls `openEngineAndSkipTranslogRecoveryFromSnapshot` → `openEngineAndSkipTranslogRecovery(syncFromRemote
= isSearchOnly())`. So it starts empty and catches up through segment replication. That method also asserts its
recovery source is `EMPTY_STORE` or `EXISTING_STORE`, which is exactly what `LocalShardStore` already produces — so
the recovery-source decision needed no change for search replicas.

### The remote store has a real fence, and `segments_only` cannot claim it

`cluster.remote_store.fencing.enabled` and `index.remote_store.fencing.enabled` exist in 3.9, and the machinery
behind them is more than a flag. `IndexShard.sealRemoteStoreFence` claims the fence for a copy through a **CAS
token on the remote translog** (`RemoteFsTranslog.sealFence`), and `sealRemoteStoreFenceForRecovery` runs it for
*every* recovery source except `PEER` — peer recovery is skipped deliberately, because a relocation target
recovers while the source is still serving at the same primary term, and sealing there would fence a healthy
primary mid-handoff.

**Correcting what this note used to say.** It claimed the fence "materially improves D8.2's outlook" because it is
a compare-and-set rather than a comparison of primary terms, so a term stuck at 1 does not defeat it. The second
half is true and the conclusion does not follow, because **this deployment cannot claim the fence at all**:

- `sealRemoteStoreFence` casts the translog factory to `RemoteBlobStoreInternalTranslogFactory` and asks it for
  **the translog repository** to put the token in (`IndexShard.java:6113-6131`). With
  `node.attr.remote_store.mode=segments_only` there is no translog repository, so there is nowhere to claim it;
- both call sites are gated on `indexSettings.isRemoteTranslogStoreEnabled()` — recovery at `IndexShard.java:3386`,
  replica-to-primary promotion at `:5958` — which is false in this mode, so neither runs;
- and `SETTING_REMOTE_STORE_FENCING_ENABLED` defaults to `false` (`IndexSettings.java:1213`), so it would be off
  even where the repository existed.

**What makes this worth more than a correction: the enforcement side already reaches segments.**
`RemoteStoreRefreshListener` consults fence ownership twice on every sync — gating segment *publication*, failing
open, and stale-segment *collection*, failing closed, on deliberately separate checks with opposite failure
directions (`RemoteStoreRefreshListener.java:249` and `:281`, with a `FenceSegmentFlow.tla` model cited for why one
shared check would be unsafe). Both are inert here only because they test `isRemoteStoreFencingEnabled()` first.

So the mechanism that would fence a superseded primary on the **segment** path exists and is one repository-shaped
gap away from being usable: the token is claimed on the translog repository, and `segments_only` has only a segment
one. **That is the finding D8.2 asks for** — it says to report a mechanism that would let a superseded primary be
fenced — and it is an upstream change rather than anything this plugin can do.

The honest position for D8.2 in this deployment is therefore **"no fencing, mitigated only by the supervisor never
moving or failing over a primary"**, which is worse than what this note previously said and is what M9 has to be
designed against. Note also that a document-replication index is not backed by the remote store here at all, so
M9's exposure is two primaries diverging locally rather than corrupting a shared remote path — a different problem
from the one D8.2 describes.

The same machinery is why the empty-primary hole in `MILESTONES.md` M7.1 is worse than it first looks: an empty
`EMPTY_STORE` primary would seal the fence like any other non-peer recovery **if the fence were reachable**, so
that copy becomes authoritative in a way the copy that still has the data cannot contest — and in `segments_only`
there is no fence to contest with either.

### The primary term already orders the remote segment store, which revises D8.2 downwards

The note above says this deployment has no fence, and for the *translog* CAS token that is true. The segment side is
in better shape than that implies, and it is worth knowing before anyone designs a path-prefix scheme to get it.

Three mechanisms, all already there:

- **Segment metadata filenames begin with `RemoteStoreUtils.invertLong(primaryTerm)`** — `Long.MAX_VALUE - term`,
  zero-padded — followed by the generation. `readLatestMetadataFile` lists them with
  `listFilesByPrefixInLexicographicOrder` and takes `get(0)`, so **the highest primary term wins, deterministically,
  at read time**. A superseded primary's metadata can never be the latest. That is a fence in the sense readers care
  about, and it is better than a per-primary path would be: both primaries write to one place and every reader
  prefers the higher term without being told which to look at.
- **The node id is in the metadata filename too**, and `RemoteStoreUtils.verifyNoMultipleWriters` throws
  `IllegalStateException("Multiple metadata files from different nodes having same primary term and generations")`
  when two node ids appear at one (term, generation). So two primaries at the *same* term — which is this
  deployment today, every term being 1 — are **detected loudly on the read path** rather than silently merged.
- **Uploaded segment data files carry a uuid** (`SEGMENT_NAME_UUID_SEPARATOR`), so two writers producing `_0.cfs`
  write distinct blobs. They cannot overwrite each other's data.

**So D8.2's "could write to the same remote path and corrupt it" is pessimistic for segments.** The honest failure
today is a loud exception plus a superseded primary uploading metadata nobody reads. What makes it safe is entirely
that the term orders the metadata — and what stops us using that is only that **the term never advances here**, which
is finding 11 and not a remote-store problem at all.

**Two consequences worth carrying.** A per-shard `index.remote_store.segment.path_prefix` is not needed: the primary
term is already a per-shard value in cluster state and already in the layout, so the "OpenSearch has no shard
settings" problem does not have to be solved. And a search replica needs no flipping between prefixes — it calls
`readLatestMetadataFile`, which follows the highest term on its own, so a primary moving needs no rolling replace of
its replicas.

### A persistent setting in fabricated metadata does reach RemoteStoreSettings — traced end to end

Asked for by the supervisor, because `DESIGN.md` §27.3's first recommendation rests on it: turning segment cleanup
off on a shard's primaries for the length of a deployment, so a rollback stays possible, using the dynamic cluster
setting `cluster.remote_store.index.segment_metadata.retention.max_count`. Five links, all checked:

1. `Metadata.settings()` is `Settings.builder().put(persistentSettings).put(transientSettings).build()`, so a
   **persistent** setting is included — transient wins a tie, which is upstream's precedence and not ours to change.
2. `ClusterApplierService` calls `clusterSettings.applySettings(event.state().metadata().settings())` on an applied
   state, **guarded by `metadataChanged()`** — see the next paragraph, which is the part that nearly made this a
   no-op.
3. The setting is `Property.NodeScope, Property.Dynamic`, which is what makes it applicable at all: `applySettings`
   only touches dynamic settings.
4. `RemoteStoreSettings` registers `addSettingsUpdateConsumer(..., this::setMinRemoteSegmentMetadataFiles)`, so the
   value lands in the node-level singleton.
5. `RemoteStoreRefreshListener` reads it **live** on each sync —
   `deleteStaleSegmentsAsync(indexShard.getRemoteStoreSettings().getMinRemoteSegmentMetadataFiles())` — so a change
   takes effect on already-running shards without reopening anything.

`-1` is the documented off switch: the setting's minimum is `-1`, its javadoc says `value < 1` disables deletion of
stale metadata, and `0` is rejected outright with *"it would delete all the data from remote segment store"*.

**The link that was missing was on this side.** `metadataChanged()` only holds on a pass that publishes, a pass only
publishes when `StateEquivalence.sameContent` sees a difference, and `sameContent` compared indices and routing
tables only — its javadoc asserted "everything else in the state is constant". So a state differing *only* by a
cluster setting compared equal, was never published, and the setting reached nothing: the node would hold the value
in the state it fabricated while the running components did not have it. Fixed, with tests, and worth remembering as
the general shape — **anything this plugin ever puts in a cluster state outside an index has to be in `sameContent`
or it does not happen.**

**One cost worth stating before this is the recommended first version:** the setting is cluster-scoped, so it stops
cleanup for *every* shard on that node, not just the one being deployed. On a node holding several tenants' shards
the blast radius of the remote-store growth is other tenants' storage, which may argue for the metadata lock sooner
than "first version" suggests, or for placing a deployment's primaries on nodes chosen for it.

### Pinning a search replica to a term, and why the pin must come with a lock

Two live primaries at different terms is not something vanilla OpenSearch ever has, and it is something this design
wants: promote a new primary at term N+1, keep the old one at N alive, and roll back to it if the upgrade goes
wrong. That makes "a search replica follows the highest term automatically" the wrong default — convenient for
failover, useless for rollback, because every replica has already moved.

**Both mechanisms needed already exist, built for snapshots.**

- `RemoteSegmentStoreDirectory.initializeToSpecificCommit(primaryTerm, commitGeneration, acquirerId)` reads a
  *chosen* (term, generation) rather than the latest. So reading an older primary's view is not new machinery.
- It goes through `RemoteStoreMetadataLockManager.fetchLockedMetadataFile(prefix, acquirerId)`, and
  `deleteStaleSegments` calls `fetchLockedMetadataFiles(METADATA_PREFIX)` and excludes what it finds — **failing
  closed**, skipping deletion entirely if the locks cannot be read. So a lock protects a metadata file and the data
  files it references from garbage collection.

**The two are not independent, and this is the part to get right.** `lastNMetadataFilesToKeep` counts metadata files
in lexicographic order regardless of which primary wrote them, so a busy new primary at term N+1 pushes the old
primary's term-N metadata out of the keep-window — at which point the data files only term N references become
eligible. **A replica pinned to term N without a lock on term N's metadata is reading files the new primary's GC is
free to delete.** That is why the snapshot path pairs them, and it is the one coupling a rollback design must not
miss.

**The per-shard granularity worry does not apply here**, which is worth knowing before anyone tries to add shard
settings upstream. Two reasons: the assignment is already per-node *and* per-shard, so it can carry a pin without
any new OpenSearch concept; and each node fabricates its own `IndexMetadata`, so even an index setting is
effectively per-node in this deployment. The residual case — two shards of one index on one node wanting different
terms — is already expressible, because primary terms in `IndexMetadata` are per shard.

Worth weighing rather than assuming: the replica's own fabricated `primaryTerm(shardId)` could *be* the pin, with
upstream honouring it on the segment read path for search-only copies, which needs no new setting at all. Against
that, reusing the term field for two meanings is subtle, and an explicit pin says what it means.

**The half still worth the fence, and where I stopped short of a claim:** deletion. `RemoteStoreRefreshListener`
gates stale-segment collection on `isRemoteStoreFencingEnabled() && isRemoteStoreFenceSupersededFailingClosed()`,
which is off here — so a superseded primary still runs `deleteStaleSegmentsAsync`. It keeps the N most recent
metadata files by the same lexicographic order, so it would retain the winner's metadata, which suggests it is
safe; but I have not traced how the referenced-data-file set is computed, and "reads are ordered, garbage collection
is argued" is exactly the asymmetry the fence exists for.

### What promotion in place actually requires of the fabricated state, and what is still argued rather than run

Three things have to hold together, and each one is a separate mechanism in the server. Read from
`../OpenSearch` rather than observed on a node — the promotion has unit coverage here but has **not** been driven on
a running cluster, because doing that needs the supervisor to demote and promote, which is section 27's work.

- **The term must strictly increase.** `IndexShard.updateShardState` activates primary mode on an unchanged term
  only if the current entry is `initializing`; a *started* replica whose term did not move falls through both
  branches, leaving a shard the routing table calls a primary whose `ReplicationTracker` is not in primary mode.
  Writes then fail with "shard is not in primary mode" and nothing says why. The increased-term branch is guarded by
  `assert newPrimaryTerm > pendingPrimaryTerm : "primary terms can only go up"`, and is what calls `bumpPrimaryTerm`,
  resets the engine to the global checkpoint and fills sequence-number gaps.
- **The allocation id must survive the role change**, or `isSameAllocation` fails and
  `IndicesClusterStateService` removes the shard and creates another — at which point the term increase is applied to
  a copy with no history and the old copy may still believe it is primary at the old term.
  `ShardRouting.moveActiveReplicaToPrimary()` is the upstream method for exactly this transition; it throws on a
  search-only copy, which is why the fabricator checks.
- **The metadata term and the routing entry must be published together**, which they are, because both come out of
  one `buildClusterState` pass. A state carrying the new routing entry at the old term is the "falls through both
  branches" case above.

What has **not** been established: whether a promoted primary's `ReplicationTracker` ends up tracking the copies the
supervisor thinks it should, given that the in-sync set fabricated here contains only the local copy. The reasoning in
MILESTONES' M9 says it will not, and that is the same hole a primary restart opens; promotion does not close it and
was not expected to.

### A metadata-only change reaches a running shard, which is what makes the ingestion pause free

Worth knowing generally and not just for ingestion: `IndicesClusterStateService.createOrUpdateShards` iterates
every shard the local routing node holds on **every** applied state and calls `updateShard` for each one, with no
equality guard on the routing entry — the only skip is `failedShardsCache`. So a published state whose routing
table is byte-identical and whose *metadata* differs still reaches each shard.

That is the mechanism behind `IngestionStatus.isPaused` costing this plugin three lines. `updateShard` calls
`shard.updateShardIngestionState(indexMetadata)`, which builds an `IngestionSettings` from the metadata and hands
it to `IngestionEngine.updateIngestionSettings`, so pausing is a fabrication change and nothing more. Two details
of that path are load-bearing:

- **it only fires for the primary**, unless `indexMetadata.isAllActiveIngestionEnabled()` — correct here, since the
  primary is the copy that consumes;
- **`IngestionStatus` is a record**, so `IngestionStatus(false)` equals `IngestionStatus.getDefaultValue()`. Setting
  it unconditionally therefore changes nothing for an index with no source, which is why it needs no guard. Had it
  been a class without `equals`, every index everywhere would have gained a metadata change.

### A consumer reset transiently starts the poller, whatever pause state it lands on

`resetStreamPoller` ends with `initializeStreamPoller(...)`, which ends with `streamPoller.start()` — and
`updateIngestionSettings` applies the settings' pause flag only *after* the reset has run. So a reset always brings
the consumer up running for a moment, and documents at the new pointer can be ingested even when the reset is
landing paused.

Found by an integration test asserting the opposite. It matters for an operator who pauses deliberately and then
resets while staying paused: they get a batch they did not ask for. "Landed paused" means the poller is stopped
once the reset has finished, not that nothing was consumed while it ran.

### A consumer-pointer reset refuses to run on a live poller, and persists into the commit

`IngestionEngine.resetStreamPoller` throws `IllegalStateException("Cannot reset consumer when poller is not
paused")`, and `updateIngestionSettings` applies a reset **before** the pause flag in the same call — so one call
carrying both a reset and `isPaused: false` cannot work on a running poller, and neither can one carrying a reset
alone. Resetting always means pause, reset, then set the state you actually want.

It ends with `flush(true, true)` to persist the new batch start pointer, which composes with M8.2's recovery: the
commit user data carries `StreamPoller.BATCH_START`, so a copy that moves after a reset resumes from the reset
point with no instruction surviving anywhere. `StreamPoller.ResetState` is `EARLIEST`, `LATEST`,
`RESET_BY_OFFSET`, `RESET_BY_TIMESTAMP`, `NONE` — the middle two are what a `reset_settings` mode maps onto, and
the value is a string either way, parsed as millis for a timestamp.

### An `IngestionEngine` validates a translog it will never use, and `segments_only` has nobody to fix it up

The exception, from a restart with a surviving data directory:

```
EngineCreationFailureException[failed to create engine]; nested: TranslogCorruptedException[translog from
source [.../0/translog/translog-1.tlog] is corrupted, expected shard UUID ETNXc9meSvuW2ibmGmrGMA but got:
oSd_8bYpQHmusojL5h1afQ this translog file belongs to a different translog]
```

A pull-based index has no translog — `IngestionEngine` uses a `NoOpTranslogManager` — and it is still required to
have a *consistent* one, because the check happens before any translog manager exists. Four links, each read from
the server's source:

- **`InternalEngine.<init>` calls `store.trimUnsafeCommits(...)` unconditionally**, then
  `Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY))` — `InternalEngine.java:276-278`. An
  `IngestionEngine` reaches it through `super(engineConfig)` at `IngestionEngine.java:79`, so "no translog" never
  gets a chance to be true.
- **`trimUnsafeCommits` takes the UUID from the last Lucene commit** and validates the local translog file against
  it — `Store.java:2242-2249`. So the message's "expected" UUID is the *commit's* and the "got" is the *file's*
  (`TranslogHeader.java:144-158`).
- **The commit is replaced from the remote store on the way in.** `innerOpenEngineAndTranslog` calls
  `syncSegmentsFromRemoteSegmentStore(false)` whenever the index is remote-store-enabled and `syncFromRemote`
  (`IndexShard.java:3390-3392`), and that method **deletes every local `segments_N` and commits the downloaded
  infos** — `IndexShard.java:6278-6295`, whose user data is copied wholesale by `Store.commitSegmentInfos`
  (`Store.java:1032-1035`). `overrideLocal=false` does not protect the commit; it only affects segment files. So
  the `TRANSLOG_UUID` in play belongs to whichever copy uploaded, not to this disk.
- **The fix-up for exactly this exists, and is replica-only.** `IndexShard.java:3404-3421`, whose own comment says
  *"when we download segments from remote segment store, we need to make sure that local translog is having the
  same UUID that is referred by the segments. If they are different, engine open fails with
  TranslogCorruptedException"* — and recreates the local translog with the downloaded commit's UUID. It sits in the
  `else` branch of `if (shardRouting.primary())`. A **primary** takes the other branch, which does something only
  when `isRemoteTranslogStoreEnabled()`.

`isRemoteTranslogStoreEnabled()` is `remoteStoreTranslogRepository != null && !isEmpty()` — `IndexSettings.java:1671`.
**Which is false in exactly the mode this whole design runs in.** A `segments_only` primary is a copy that downloads
a foreign commit and then falls through both branches with nothing rewriting its translog.

Worth noting that upstream already knows an ingestion source has no checkpoint worth loading — two lines above the
call that fails, `openEngineAndRecoverFromTranslog` skips `loadGlobalCheckpointToReplicationTracker()` when
`useIngestionSource()` (`IndexShard.java:3306-3310`). The engine constructor then reads the same checkpoint out of
the translog file anyway.

### A remote-store-backed copy does not resume from its own last commit

The correction the exception above forced, and it is worth separating from it because it is true whether or not
anything throws. `syncSegmentsFromRemoteSegmentStore` runs on **every** engine open of a remote-store-enabled
index, on the `EXISTING_STORE` path as much as any other — `internalRecoverFromStore` calls the no-arg
`openEngineAndRecoverFromTranslog()`, which passes `syncFromRemote = true` (`IndexShard.java:3286-3290`).

So "recover from local bytes" does not mean the local commit is what the engine opens on. Every local `segments_N`
is deleted and the downloaded infos are committed in its place. For a pull-based index that means
`StreamPoller.BATCH_START` — the consumer position M8.2 resumes from — is the **last uploaded** commit's, not the
last local one. Those are the same thing on a busy shard that refreshes and uploads constantly. They are not the
same thing on an idle or paused one, which is where the gap is widest and where a resume is most likely to be
watched.

### Every ingested message must carry `_id`, which is what makes a replay survivable — and invisible

`MessageProcessorRunnable` rejects a message with no `_id` field outright — `"ID field is missing. Invalid
message."` — so a pull-based index's documents are always id-addressed rather than auto-generated. That is worth
knowing well beyond ingestion, because it decides what re-consuming a range of the stream costs: **a replay
re-indexes the same documents rather than duplicating them.**

Which is what makes the in-memory idempotence of `IngestionResets` honest rather than lucky. Two exceptions to keep
in mind before relying on it:

- **`_op_type: create`** takes the `isCreateMode` path, so a replay is a version conflict per document rather than
  an overwrite, and `streamPoller.getErrorStrategy()` decides whether that drops or blocks. A blocking policy on a
  create-mode stream turns a replay into a stalled shard.
- **`_version` in the payload** switches the operation to `VersionType.EXTERNAL`, which makes a replay a no-op
  rather than a rewrite — better, and worth knowing it is the tenant's choice rather than the default.

### How to see a consumer reset at all, since id-stability hides it

The other face of the note above, and it cost a real debugging session to notice. Re-consuming a range rewrites
the same ids with the same content, so **a reset that worked and a reset that did nothing leave an identical
index** — same documents, same count, same everything. The obvious check, "did the document count change", answers
neither question.

The way through, for the `ingestion-fs` source: **edit the payloads behind the stable ids, then reset.** Change
the ndjson so the documents read `foo modified`, `bar modified`, `baz modified`, reset the consumer to offset 2,
and search. Getting back `foo`, `bar`, `baz modified` says three things at once that nothing else does — the
pointer landed exactly where it was sent, the poller was running afterwards rather than left paused, and it
consumed *from* the pointer instead of replaying the file.

Two things to carry away from it:

- **Offsets are 0-based line numbers for the file source.** Offset 2 is the third line. The kind of off-by-one
  that reads as "the reset went to the wrong place" when it went exactly where it was told.
- **The technique does not transfer to production.** Rewriting history is a property of a file; Kafka does not
  let a producer modify an earlier message. On the deployment this is built for there is no way to see a replay
  from the index afterwards, so anything that needs to observe a reset has to watch the consumer position while
  it happens.

## Tests are JUnit 4, and this is not a preference

The first real `./gradlew build` found a forbidden-APIs failure, and pulling on it found something worse:
**the Gradle build was not running any of these tests.** They were written for JUnit 5, and:

- Nothing in the OpenSearch build calls `useJUnitPlatform()`. The only occurrence in the whole repository is
  in `buildSrc/build.gradle`, for buildSrc's own tests. A Gradle `Test` task defaults to JUnit 4, which
  cannot see a Jupiter annotation — so discovery found zero tests.
- The tests still *compiled*, which is why this was invisible: `jupiter-api` arrived transitively through the
  shared node-contract fixtures. Compiling against an assertion library and having a runner are different
  things.
- `PluginBuildPlugin` adds `test:framework` to `testImplementation`, which brings `junit:junit:4.13.2`,
  `randomizedtesting-runner` and `lucene-test-framework` — and configures `testingConventions` to expect a
  unit test named `*Tests` to extend `LuceneTestCase`.

Gradle 9 fails a `Test` task that discovers nothing, so this would have surfaced eventually as
`No tests found` — after the precommit failures were cleared, and with no hint that the cause was a test
framework rather than a filter. Everything is now `public class FooTests extends OpenSearchTestCase` with
`public void testSomething()` methods.

Five things learned converting it, all of which cost time:

- **`org.junit.Test` is itself a forbidden API** here: *"Just name your test method testFooBar"*
  (`opensearch-test-signatures.txt`). So the annotation is not the migration target — the JUnit 3 naming
  convention is. That file is worth reading before writing tests: it also bans hardcoded `@Seed` and
  `@Repeat`, `@Ignore` in favour of `AwaitsFix`, and `Math.random()`.
- **JUnit 4 puts the assertion message first.** Mechanical conversion turns `assertTrue(cond, "why")` into a
  compile error, which is the good case. The dangerous case is three strings:
  `assertEquals(expected, actual, "why")` compiles perfectly as
  `assertEquals(message, expected, actual)` — silently comparing the actual value against the message. One of
  those got through and failed with a baffling diff. Grep for `assertEquals(.*(), ".*")` after any such
  conversion.
- **Assertions are on** (`-ea`), because `LuceneTestCase` refuses to run without them. That immediately
  failed two hand-built `ClusterState` fixtures with *"Primary shard routing can't be null for
  non-search-only indices"* — a `RoutingNodes` invariant. They were invalid states that had been quietly
  passing. This is the single best thing the conversion bought, and it has a consequence for milestone 7:
  a node holding only a search replica cannot build a valid cluster state without either naming the
  primary's node or setting `index.blocks.search_only`, which is what `cluster-etcd` chose.
- **`OpenSearchTestCase`'s static initialiser does a great deal**, and every piece of it needs a jar:
  log4j-core, joda-time, JNA, reactor-core, zstd-jni. It also runs `JarHell.checkJarHell` over the entire
  classpath and **refuses to start if two jars share a class** — which `libs/agent-sm/agent` and
  `libs/agent-sm/bootstrap` do, since the agent bundles the bootstrap library. `tools/jbuild.sh` therefore
  deduplicates by jar name and skips the agent bundle.
- **`tests.system_call_filter=false`** is needed in this sandbox: `sandbox_init()` is denied, and the filter
  logs a warning on every run otherwise.

The shared conformance checks had to change shape for this. They were an abstract JUnit 5 class; an
annotation one side's runner ignores produces no error, just silence. They are now a list of named checks
(`NodeContractChecks`) that each side runs with its own runner — `fake-node` as a JUnit 5 `@TestFactory` with
per-check reporting, this plugin as one JUnit 4 test calling `runAll`. A check added in the supervisor still
runs here with nothing edited, which was the only property that mattered.

## The sandbox

Same environment the supervisor's `WORKING_NOTES.md` describes, and its notes hold here. In short:

- **Gradle runs in here now**, as of 2026-09-17 — an allowlist change, not anything about the build. It used
  to fail creating its file-lock service because binding a socket was denied, and the note that stood here
  said so. `./gradlew build` passes, and it ran the same 324 tests as `tools/jbuild.sh` across the same 26
  classes: the two harnesses agree test for test, which is worth knowing when one of them says something
  surprising.

  *2026-09-29: `tools/jbuild.sh` is removed; Gradle is the only build. What follows about it is history.*

  **Prefer Gradle now, and keep `jbuild.sh` for the inner loop.** Gradle is the only one that runs javadoc,
  forbidden-APIs, `testingConventions`, packaging and the license checks — and the first run after the
  allowlist change found four things that had been sitting in the tree precisely because nothing could look:
  a `{@link PublishScheduler}` pointing at a class renamed to `ScheduledPasses`, an `@param coordinated`
  against a parameter named `coordinatedShards`, and two `String#formatted` calls that forbidden-APIs rejects
  for using the default locale. None was a bug a test would ever have caught, and all four had survived
  months of green test runs.

  `jbuild.sh` is still much faster and still compiles against `../OpenSearch`'s built jar rather than a
  published artifact, which matters when the server change you depend on is local. Say plainly which of the
  two produced a result.

  **The forbidden-APIs signatures are worth reading up front** — `opensearch-test-signatures.txt` and
  `jdk-signatures.txt` — rather than discovering them one task at a time. `String#formatted`,
  `System.out.println` in a `main`, and `Files.createTempDirectory` in a test have all been caught here. And
  `validateNebulaPom` wants publishing configuration that a plugin nobody publishes to Maven does not have;
  `cluster-etcd` disables it too, in an `afterEvaluate`.
- **A real node CAN be started in here, and this note said otherwise for months.** `OpenSearchSingleNodeTestCase`
  starts one under Gradle — index creation, search, shard recovery, pull-based ingestion, all of it. What was
  actually true is narrower: **`tools/jbuild.sh` cannot start a node**, and while it was the only harness available
  the two were indistinguishable. Correcting it is worth more than it looks, because "nothing about a running node
  is testable" is the premise behind every *what is not proven* paragraph in `MILESTONES.md`, and the first thing
  tried against it — `IndicesServiceIngestionPollersTests`, a reset against a real `IngestionEngine` — worked
  first time and then failed correctly when its fix was reverted.

  Still true: no listening sockets, no raw TCP, no Unix sockets, no Maven Central, no containers. A single-node
  test needs none of them. **Put the risk in pure functions anyway** — `NodeState.buildClusterState` is a pure
  function of (previous state, desired state) and should stay one — but reach for a node when a seam genuinely
  needs one, rather than writing another paragraph about why it cannot be tested.

  Three things that cost a cycle each when writing the first one:

  - **The test task only includes `**/*Tests.class`.** A class named `*IT` is compiled and never run, and Gradle
    reports `No tests found for given includes` rather than anything about naming. `OpenSearchSingleNodeTestCase`
    subclasses are conventionally `*Tests` in OpenSearch anyway; `*IT` belongs to an `integTest` task this
    repository does not have.
  - **A pull-based index must be `index.replication.type: SEGMENT`.** `IndexMetadata` refuses it with document
    replication unless `index.ingestion_source.all_active` is set, because an `IngestionEngine` keeps no translog
    for a docrep replica to follow — the same reason `segments_only` exists.
  - **An ingestion source needs a `Plugin` implementing `IngestionConsumerPlugin`**, returning a factory keyed by
    an upper-case type name. The server's own `FakeIngestionSource` lives in OpenSearch's test sources and is not
    published, so it cannot be depended on from here; `ListIngestionSource` is the local equivalent, and its
    payloads are rewritable on purpose.
- **An OpenSearch checkout must be installed to mavenLocal** (`./gradlew publishToMavenLocal` there) for the
  Gradle build to resolve. **Do not let a test here depend on an unmerged
  server change**: `ClusterlessIndexSettingsTests` called `IndexScopedSettings.getRegisteredSettings()`, which
  lives on an OpenSearch branch, and this whole build went red the day that branch stopped being checked out.
  A test that can be broken by which branch a sibling repository is on is a test that will be.
- **Commits are signed**, and signing needs an interactive authorisation the
  sandbox cannot answer, so `git commit` hangs rather than failing. Stage the work, write the message to a
  file, and let a human run `git commit -F`.

## Two `./gradlew run` invocations collide on ports

`RunTask.beforeStart()` hardcodes them:

```java
int httpPort = DEFAULT_HTTP_PORT;        // 9200
int transportPort = DEFAULT_TRANSPORT_PORT;   // 9300
...
firstNode.setHttpPort(String.valueOf(httpPort));
```

and it *overwrites* whatever the build file set, so a second cluster cannot be moved out of the way from this side.
Which means `./gradlew runCoordinator` alongside `./gradlew run` claims 9200 and 9300 twice.

**Today's answer is `-Pclusterless.coordinating=true`**, which gives the run cluster's nodes both roles. That is a
real configuration rather than a dodge — OpenSearch's own default is that every node can coordinate, the tenant
roles in an assignment are a set precisely so a node can be told it does both, and it exercises the union path
`CoordinatorState` was written for. A dedicated coordinator is still the right production shape; it is just not
required to test the routing.

**If RunTask is patched**, the shape that matches its existing overrides (`--data-dir`, `--preserve-data`,
`--keystore-password`) is two more options:

```java
@Option(option = "http-port", description = "First HTTP port to assign; nodes take consecutive ports from here")
@Option(option = "transport-port", description = "First transport port to assign")
```

One detail in that method is easy to miss and would produce a confusing cluster:

```java
firstNode.setting("discovery.seed_hosts", LOCALHOST_ADDRESS_PREFIX + DEFAULT_TRANSPORT_PORT);
```

The seed host is built from the **constant**, not from the port the node was just given, so a port-shifted cluster
would seed discovery at the wrong address. It does not matter for clusterless nodes — they never discover anything
— and it would matter for everyone else, so a patch should use the base transport port it was configured with.

A per-node settings prefix (`tests.opensearch_node.…`, alongside the existing `tests.opensearch.` that
`additionalSettings` applies to every node) is a separate and more general capability. Worth keying on the **node
name** rather than an index, since testClusters already names nodes `<cluster>-<n>` and an index silently changes
meaning when `numberOfNodes` does.

## A store revision is only a change signal if it is never reused

The file-backed store used to number revisions **per key, derived from the key's own file**, so deleting a key
restarted its count at 1. That is invisible until something caches on revision equality — which every reader in
these three services does, because the `KeyValueStore` contract said revisions are "monotonically increasing" per
key and comparable for equality, and said nothing about a key being deleted and written again.

What it cost: an operator deleted and recreated `my-index-2` a few times, the recreated document came back at
revision 1, the supervisor's `RevisionCache` already had revision 1 for the *previous* incarnation, and its desired
state froze on the dead index's uuid for ever. This node was right and said so on every heartbeat for 143 passes —
`outcome: failed`, *"the document at [my-index-2] is index [EfL8…], so the assigned index is not present yet"* —
which is exactly what `IndexConfig.matches` is for. **The node's refusal was correct and the diagnosis still took
reading three repositories**, because nothing in the store or the supervisor pointed at a reused revision.

Fixed in `../opensearch-kv-store`: the contract now states that a revision is never reused for a key including
after deletion and says why, the conformance suite has two cases for it, and `FileSystemKeyValueStore` issues
store-wide revisions from a persisted counter. etcd's `mod_revision` and ZooKeeper's `mzxid` already satisfied it —
`ZooKeeperKeyValueStore` rejects `Stat.getVersion()` in its javadoc for this very hazard — so **the trap existed
only in the backend all three services are developed against**, which is worse than it sounds.

Two things to keep in mind here:

- **`config_revision` is this comparison, wearing a different hat.** `IndexConfig.revision` comes from the store
  and travels into the inventory, where the supervisor compares it to decide whether a copy is on the current
  configuration. It rested on the same unstated guarantee. Compare it for **equality only** — the contract still
  permits gaps, and the filesystem store now deliberately leaves them after a crash.
- **The same trap exists one level up, in an assignment's `version`.** It counts up from the document already in
  the store, so deleting `nodes/assignment/<id>` restarts it at 1 — and the supervisor reaps that document now. A
  node reaped while merely unreachable comes back reporting version 11 against a fresh version 1. Nothing here
  compares assignment versions and nothing should: "skip an assignment no newer than the last one applied" is an
  obvious-looking optimisation that would freeze such a node for ever. `AssignmentPoller`'s javadoc defends it,
  because the next person to read that loop will think of it. The supervisor found this by checking its own side
  after finding 10 and corrected an ordered reading of `applied.assignment_version` in `NODE_CONTRACT.md`.
- **When an index will not come back, read the store before reading the code.** The keys are files:
  `plugin/build/clusterless-store`, percent-encoded, with an 8-byte big-endian revision prefix. Comparing the uuid
  in `tenant/<t>/indices/<name>` against the uuid in `nodes/assignment/<id>` took one command and was the whole
  diagnosis.

## Reuse the store between runs, deliberately
Every bug in this plugin that reached a running deployment came from **state left over from an earlier run**, and
the reflex to start from a clean store is what hides them:

- an index whose files were retained by a process that has since exited, which nothing was tracking (M6);
- a primary returning to a node whose data directory had been wiped, over a repository that still held its
  segments (M7.1);
- projections for nodes that no longer exist, several of them claiming `127.0.0.1:9301`, because a wiped data
  directory produces a *new* node id on the same port (M8).

The last of those found a real defect rather than noise: `NodeStatus.isReachable()` and `isAddressable()` existed,
were tested, and were **called by nothing**, so a coordinator routed to nodes the supervisor had given up on — and
when two ids claimed one endpoint, the one dropped was whichever `DiscoveryNodes.Builder` happened to see second,
which could be the live one.

A clean store tests the happy path of a system whose entire design is about disagreeing observations of a fleet
that changes underneath you. So: **do not wipe `plugin/build/clusterless-store` between runs unless the point of
the run is a cold start.** The accumulated mess is the interesting input, and it is free.

The same argument does not extend to the *data* directory, which is a different axis: `--preserve-data` tests a
returning node and wiping tests a replaced one, and both are worth doing on purpose.

## Verify against something real

`tools/supervisor-sees-me.sh` is the habit that caught the most here: publish with the real publisher, run
a **real supervisor**, and read what it made of the documents. It found two things a unit test could not,
because unit tests and implementation share the author's assumptions:

- the filesystem store prefixes each value with a revision header, so a document hand-written into the store
  directory with an editor parses as garbage — and is reported under `unreadable` rather than failing loudly;
- and `unreadable_documents: 0` in the convergence projection is the assertion worth watching, because it is
  the supervisor saying it could read everything this node wrote, in its own parser rather than ours.
