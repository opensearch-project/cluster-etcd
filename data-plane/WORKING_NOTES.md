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

- **Gradle cannot run in here** — it fails creating its file-lock service, because binding a socket is
  denied. Use `tools/jbuild.sh`, and say plainly which of the two produced a result. The Gradle build does
  pass on a machine that can run it, and the two now agree test for test; what `jbuild.sh` still cannot tell
  you is anything about plugin packaging, forbidden-APIs, `testingConventions` or the license checks, and
  three of those four have already caught something real.

  **Both of the build's own complaints so far needed a human, and neither was about the code.** The
  forbidden-APIs failures — `System.out.println` in a `main`, `Files.createTempDirectory` in a test — are in
  `opensearch-test-signatures.txt` and `jdk-signatures.txt`, which are worth reading up front rather than
  discovering one task at a time. And `validateNebulaPom` wants publishing configuration that a plugin
  nobody publishes to Maven does not have; `cluster-etcd` disables it too, in an `afterEvaluate`. Turning it
  off is the right call until there is somewhere to publish to, and then it is a real `publishing` block
  rather than a smaller edit.
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
