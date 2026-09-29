# The data plane — instructions for the session that owns it

An OpenSearch plugin that fabricates each node's `ClusterState` from desired state read out of a shared key/value
store, with no cluster manager anywhere. One Claude session owns this component, and one owns each of the others;
this file is what that session needs to know that the component's own documents do not say.

## Read these first, not the conversation

- **`MILESTONES.md`** is the source of truth for progress. Every milestone records what was built, what a real run
  proved, what is not proven, and which cross-component findings came out of it. Do not reconstruct status from
  memory or from a summary.
- **`WORKING_NOTES.md`** — what OpenSearch 3.9 actually does, with file and line references. Most entries cost hours.
- **`REQUIREMENTS.md`** — the D-numbered requirements and the open questions, including what was deliberately not
  taken.
- **`BORROWING_FROM_CLUSTER_ETCD.md`** — the survey of the earlier implementation of the same idea.
- The supervisor's **`NODE_CONTRACT.md`** is the wire format this plugin reads and writes; the supervisor owns it.

## What this component owns, and what it does not

**Owns:** everything a node concludes from the documents — the fabricated cluster state, which copies it builds and
which it declines (always with a reason reaching `applied`), what it publishes about itself (registration,
heartbeat, shard inventory), and the node-side half of every instruction (ingestion pause and reset, primary terms).

**Does not own:** placement, promotion, term issue, or any decision about which node holds what — that is the
supervisor, and a node that decided it would be deciding which supervisor it obeys. Index configuration is the
control plane's. Store semantics (revisions, compare-and-set, the no-reuse guarantee) are the kv-store's.

When this side finds something another component must change, **write it up rather than change their code**: for
the supervisor, a numbered entry in its `FINDINGS_FROM_THE_DATA_PLANE.md` — what happened, why it is wrong, where the
code is, what a fix must be careful about, a recommendation, and the tests worth having. Eleven findings have gone
through that loop. Contract changes additionally get an entry in `NODE_CONTRACT.md`'s findings list. The user carries
findings between sessions; the other session replies inline in the same file.

**Never stage or sweep another component's in-progress files.** Their sessions stage their own. Under a single
repository this matters more, not less: scope every `git add` to this component's directory.

## How to work here

### Commits: stage, write the message, hand over the command

Commits are signed, and signing blocks on an interactive authorisation the sandbox cannot
answer — running `git commit` hangs the turn and produces nothing. So: make the change, `git add` the paths, write
the full message to `/tmp/dp-<topic>.txt`, and give the user the exact `git commit -F /tmp/dp-<topic>.txt`. This is
not about autonomy; changes may be made and staged without asking.

Write the message as though committing yourself, in the established style: a subject saying what changed, then prose
on *why*, what was learned, how it was verified, and **what is not proven**. Correct earlier commits' claims in a
later message when they turn out wrong — a committed message is what a later reader believes.

### Say which tool produced a result

Separate what was verified from what was argued, every time, and name the source: `./gradlew`, `tools/jbuild.sh`, a
file and line in OpenSearch, or the user's own output. Never present a reading of the server's source as an
observation of its behaviour. The "what is not proven" paragraph has repeatedly been the useful one — the user's next
run tends to go straight at it.

What the sandbox can do, since that premise was wrong for a long time:

- **Gradle runs** (`./gradlew build`, `./gradlew :plugin:test --tests "*Foo*"`). Prefer it when a claim needs proving:
  it runs javadoc, forbidden-APIs and packaging as well as the tests.
- **A real node starts** under `OpenSearchSingleNodeTestCase` — index creation, search, recovery and pull-based
  ingestion all work. `IndicesServiceIngestionPollersTests` is the example.
- `tools/jbuild.sh` is faster and compiles against a locally built OpenSearch; use it when depending on an unpublished
  server change.
- **Test classes must be named `*Tests`.** The test task includes only `**/*Tests.class`; an `*IT` class is silently
  never run and the build stays green.
- Multi-node runs (`./gradlew run -Pclusterless.nodes=5 ...`) are the user's, on their machine.

### Test the second pass, and prove the test fails without the fix

The fabrication is a pure function of (previous state, desired state) run every poll interval, and **every defect
that reached a running node was in the second call**: a fatal `AssertionError` from a settings version bumped for a
no-op change, a search-replica count that grew each pass, an in-sync set that grew without bound, routing rebuilt
each pass so no two states matched. For anything touching fabricated state, assert that the same inputs twice give
the same result — versions and allocation ids included — and that `StateEquivalence.sameContent` holds.

Then **revert the fix, run the one test, watch it fail with the reported symptom, restore.** This has caught vacuous
tests at least four times (a fixed point that also held without the fix; an assertion satisfied by a hardcoded
value). Say in the commit message what reverting showed.

Reuse the filesystem KV store between runs rather than starting clean — leftover state is where these bugs live.

### The user

The author of this design, of `cluster-etcd` (the earlier implementation), and of the upstream
`node.attr.remote_store.mode=segments_only` attribute, added for exactly this. Their corrections about OpenSearch
have been right every time: when they say something about the server's behaviour, look it up to find the exact
mechanism, not to decide whether they are right. They also push back on design, and it is worth hearing — e.g. a
primary under remote-store replication should just publish and need not know its search replicas; anything upstream
that says otherwise is an upstream bug.

Before raising an alarm about the user's other work (branches, stashes, uncommitted changes), check `git branch`
and `git stash list` first. A false alarm about lost work was raised once; it had been moved to a branch.

## The shape being built toward

Decides arguments that would otherwise be settled by what is easiest on a laptop:

- **Pull-based ingestion with segment replication is the intended path.** Kafka in production; the `ingestion-fs`
  source (one ndjson file per shard, line number as offset) is for testing. `segments_only` exists because an
  `IngestionEngine` keeps no translog.
- **Document replication is supported but discouraged**, for small tenants who will not be persuaded. Its job is to
  work, not to scale; re-recovering a replica is an acceptable cost there.
- **Rolling deploys drain zone by zone**: primaries on departing nodes are demoted and replicas elsewhere promoted,
  so every primary moves twice per three-zone deploy. Promotion is the normal path, not a failure path. The user
  also wants rollback to the old primary, which motivates an upstream search-replica term pin.
- **Coordinators get dedicated capacity at ~50+ nodes**; colocating is a small-deployment convenience.
- **The tenant arrives in an `x-tenant-id` header** injected by an auth layer outside OpenSearch.
  `clusterless.tenant` (a single-tenant node) remains available.
- **The audience is search platform teams** running OpenSearch for many internal tenants; see the control plane's
  documents.
- **Live shard splitting is not the plan** — stand up a new index, restore, delete out-of-shard documents, resume
  from the checkpoint.

## Where things stand (2026-09-29)

M1–M8.2 are done and confirmed on real nodes. M9 (document replication) is in progress; see `MILESTONES.md` for
the detail. The last two commits here were the node half of `primary_terms` (assigned term used, in-place promotion
via `moveActiveReplicaToPrimary`, `contract_features: ["primary_terms"]` declared) and the conformance adapter
reporting fabricated terms.

**Next, awaiting the user's go-ahead:** the in-sync gap. A promoted or restarted docrep primary fabricates an in-sync
set of only itself, so started replicas are neither tracked nor replicated to, and silently go stale. The
recommended fix, proposed and not yet approved for promotion: a replica rebuilds itself as `INITIALIZING` when its
primary's identity changes — new node, new `ephemeral_id`, or new allocation id — so ordinary peer recovery marks it
in sync. Not recommended: declaring every visible replica in sync, which guesses at exactly what the set exists to
know. Persisting `in_sync_allocation_ids` is the door left ajar, not a priority.

**Waiting on the supervisor:** `retain_remote_history` and `abandon_remote_terms` (DESIGN.md §27). Add each to
`Registration.IMPLEMENTED_FEATURES` only once this side acts on it — declaring a feature is a promise the supervisor
relies on. The drain flow (planned demotion) is not built there yet; promotion happens only on a lost primary.

**Upstream, owned by the user:** removing the `RoutingNodes` assertion that every shard has a primary (load-bearing
for search replicas *and* for docrep replicas, see `PeerRecovery`); the `InternalEngine.trimUnsafeCommits` guard for
ingestion engines (submitted); a search-replica term pin for rollback. The user's `AbstractScopedSettings` change
is pending upstream, kept apart deliberately.

## Paths after the move to one repository

Written before the move. The tooling assumes sibling repositories and will need adjusting — the session doing the
merge may already have done it:

- `settings.gradle` has `includeBuild('../opensearch-kv-store')` and `includeBuild('../opensearch-supervisor')`.
- `tools/jbuild.sh`, `tools/supervisor-sees-me.sh` and `tools/supervisor-assigns-me.sh` default `KV_STORE`,
  `SUPERVISOR` and `OPENSEARCH` to `$ROOT/../<repo>`; each is overridable by environment variable.
- `build.gradle` and `plugin/build.gradle` refer to `../OpenSearch` for the server snapshot and the `ingestion-fs`
  plugin distribution.
- The conformance suite is the supervisor's `fake-node` test fixtures (`NodeContractChecks`), run here by
  `PluginNodeContractTests`. When another session's tree is mid-refactor and does not compile, it breaks this build
  too; isolate against their last commit (`git archive HEAD | tar -x -C /tmp/sup-head`, then
  `SUPERVISOR=/tmp/sup-head ./tools/jbuild.sh`) rather than touching their files.
