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

## The sandbox

Same environment the supervisor's `WORKING_NOTES.md` describes, and its notes hold here. In short:

- **Gradle cannot run** — it fails creating its file-lock service, because binding a socket is denied. Use
  `tools/jbuild.sh`, and report honestly that `./gradlew build` is unverified.
- **No listening sockets, no raw TCP, no Unix sockets, no Maven Central, no containers.** So **a real
  OpenSearch node cannot be started here**, which is the sharpest limitation for this repository
  specifically: everything about talking to the store is testable, and nothing about a running node is.
  Put the risk in pure functions and test those exhaustively — `NodeState.buildClusterState` is a pure
  function of (previous state, desired state) and should stay one.
- **`../OpenSearch` must be built once** (`./gradlew assemble`) for `tools/jbuild.sh` to find jars, and
  installed to mavenLocal for the Gradle build to resolve.
- **Commits are signed**, and signing needs an interactive authorisation the
  sandbox cannot answer, so `git commit` hangs rather than failing. Stage the work, write the message to a
  file, and let a human run `git commit -F`.

## Verify against something real

`tools/supervisor-sees-me.sh` is the habit that caught the most here: publish with the real publisher, run
a **real supervisor**, and read what it made of the documents. It found two things a unit test could not,
because unit tests and implementation share the author's assumptions:

- the filesystem store prefixes each value with a revision header, so a document hand-written into the store
  directory with an editor parses as garbage — and is reported under `unreadable` rather than failing loudly;
- and `unreadable_documents: 0` in the convergence projection is the assertion worth watching, because it is
  the supervisor saying it could read everything this node wrote, in its own parser rather than ours.
