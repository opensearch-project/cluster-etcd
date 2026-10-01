# The supervisor — guide for the session that owns it

This file is for the Claude session that works on this component. It holds what cannot be read off the code
and the design documents: what this component owns, how work is done here, and where it stands. For *what the
system is*, read `README.md`, then `DESIGN.md`, `NODE_CONTRACT.md` and `SUPERVISOR_KEYS.md`. `WORKING_NOTES.md`
has the long-form environment gotchas.

## What this session owns

One session per component: data plane, control plane, supervisor, key-value store. Each owns the judgement of
what its component should and should not do. This session owns:

- **The supervisor** — `supervisor-model`, `supervisor-core`, `supervisor-app`, `supervisor-integ`, `fake-node`.
- **`NODE_CONTRACT.md`**, the seam with the data plane, and **the conformance suite** in
  `fake-node/src/testFixtures/.../nodecontract`, which the data plane consumes. A format change is made here
  first, with a check that fails on the reading side until the other side catches up.
- **`SUPERVISOR_KEYS.md`**: every key this service writes.

Components talk only through documents, never through each other's code:

- **Findings from the data plane arrive in `FINDINGS_FROM_THE_DATA_PLANE.md`.** Answer each item in place with a
  status block at the top of the item (`> **Closed.** …`, `> **Supervisor half done.** …`), saying what was done
  and anything the other side must do next.
- **Proposals for another component are written as a document in its tree**, not as code changes. See
  `../control-plane/docs/proposed-tenant-registry.md`, and the data plane's `REQUIREMENTS.md` and
  `MILESTONES.md`, which this session wrote.
- Another session's uncommitted edits to a shared file are theirs. Leave them out of your commit. Check
  `git diff` on every file before staging it.

Ownership decisions already made. Don't reopen them without a reason:

- **Desired state is the control plane's.** The supervisor never invents intent. It projects status into
  `tenant/<t>/status/**`, reaps tasks it completed, and keeps its own state under `supervisor/`. That includes the
  primary-term counters, the one piece of state it cannot recompute (`DESIGN.md` §28.6).
- **The supervisor has no access to snapshot or remote-store repositories.** So R5.3 is deferred, and any
  remote-store deletion is done by a data plane node, told to through its assignment.
- **Instructions that must run once** (ingestion resets, term abandonment) carry an id and are acknowledged
  through `applied`. Everything else in an assignment is level-triggered desired state (`DESIGN.md` §21.2).
- **Contract changes are additive.** Add sibling fields rather than changing an existing value's shape. Readers
  tolerate what they don't know. Capabilities are declared in `contract_features`, because `applied` cannot
  distinguish "used the field" from "ignored it" (§28.3).
- **Monitoring: detection stays in the supervisor; judging and delivering alerts happen outside it.**
  `MONITORING.md` §9 has the two guardrails.

## How work is done here

- **Building.** Gradle and Docker both work in the sandbox, which the session that crashed never knew:
  `../gradlew build --offline` from here (or `./gradlew build --offline` at the repository root, for every
  component), and `../gradlew test` runs everything, including the integration tests against etcd and ZooKeeper
  under Testcontainers (about two minutes). Use `--offline`: Maven Central is unreachable, and the local caches
  have everything. Don't report a change as green without running the Gradle build.
- **Committing.** Commits are signed. A plain `git commit` works from the sandbox now, but
  signing has hung here before, waiting on an approval that never arrived. So commit with a two-minute timeout, and
  if it hangs, stop it, leave the change staged with the message in `.git/<TOPIC>_MSG`, and ask Michael to run
  `git commit -F .git/<TOPIC>_MSG`. Commit only when asked. Message style: prose explaining *why*, the problem
  before the change, and what was verified and how. End with `Signed-off-by: Michael Froh <m_froh@apple.com>`.
- **Several sessions share one checkout.** Never move HEAD: no `git checkout`, no `git checkout -b`. To put work
  on a branch, build the commit with a temporary index (`GIT_INDEX_FILE`, `read-tree`, `write-tree`,
  `commit-tree`, `git branch`). That commit is unsigned, so say so.
- **Verify, don't assume.** Check each new regression test by reverting the fix and watching it fail. Check
  claims about OpenSearch against its source (an OpenSearch checkout, outside this repository), including claims from the data plane's notes. Where
  numbers can be measured, measure them and record them in the doc (see `DESIGN.md` §12.1).
- **This is headed for open source.** No company-internal or vendor product names in code, docs or commit
  messages. The sign-off address is the only exception.
- **`DESIGN.md` is mostly appended to.** When a section is overtaken, add a short "superseded by §N" note to it
  rather than rewriting it (see §26 → §27). Section numbers are cited across repositories.
- **Docker for integration tests.** Testcontainers finds the daemon through `docker context inspect`, and
  `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` must stay `/var/run/docker.sock`. The reason is in `WORKING_NOTES.md`;
  the wrong setting looks more correct.

## Where things stand (2026-10-01)

**Last landed:** findings 12 and 13, both closed on both sides. Allocation ids and the in-sync set are carried
from the inventory into `status/indices` for document replication, `Promoter` promotes only a replica the lost
primary last called in sync (`DESIGN.md` §28.10), `fake-node` publishes both, and the conformance suite gained
`readIndexStatus`, a reading-side check of `status/indices`. An index that does not state its replication type is
`DOCUMENT`, as in OpenSearch. Before that: the review fixes for the PR that published v2, and primary terms and
promotion (§27–28, finding 11).

**Open threads, roughly in priority order:**

1. **Somewhere to record that a drain or dual-run is intended.** Open question 11 (announced deployments) and
   §28.9. It blocks three things: intentional demotion (a drain), the rollback dual-run (§27.4), and exempting
   an intended dual-run from `PRIMARY_CONFLICT`. This is the next design task.
2. **The rest of §28:** `retain_remote_history` and `abandon_remote_terms`, the scoped metadata lock (§27.5),
   and the upstream search-replica pin (§27.3). None of it is built.
3. **`NodeStatus` has no reading-side conformance check.** `status/indices` has one now; `status/nodes` is the
   other document the data plane reads.
4. **No way to promote an out-of-sync replica on purpose.** With no in-sync replica left, a docrep shard stays
   without a primary; OpenSearch has `allocate_stale_primary` for this, and nothing here does.
5. **Two projections still read the configured copy count rather than the authorised one:** `Differ` and
   `StatusComputation` (§15.2). Health is per shard now, but within a shard copies are not counted by role.
6. **Long-standing:** R5.3 snapshot metadata, cross-pool migration, the dedicated-pool ceiling, rollover, an
   ordinary replica of a segment-replication index (open question 10), and the heat signal (proposed in
   `NODE_CONTRACT.md`).

## Paths in the one repository

This component is `supervisor/` in a repository whose root builds every component. Its neighbours are
`../kv-store` (included as a composite build), `../data-plane` and `../control-plane`; the wrapper is `../gradlew`.
An OpenSearch checkout, where one is needed for reading server source, is outside the repository and named by the
reader rather than assumed. Some docs written before the move still say `../opensearch-*`; read those as the
directories above.
