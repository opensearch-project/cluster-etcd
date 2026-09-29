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
  `../opensearch-control-plane/docs/proposed-tenant-registry.md`, and the data plane's `REQUIREMENTS.md` and
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

- **Building.** The sandbox cannot run Gradle or Docker. `./tools/jbuild.sh` compiles everything, runs every
  unit test, and typechecks `supervisor-integ`. Michael runs `./gradlew build` and
  `./gradlew :supervisor-integ:test`, which runs Testcontainers against etcd and ZooKeeper.
- **Committing.** Commits are signed, which hangs in the sandbox. Stage exactly your own
  files, write the message to `.git/<TOPIC>_MSG`, and ask Michael to run `git commit -F .git/<TOPIC>_MSG`. Message
  style: prose explaining *why*, the problem before the change, and what was verified and how. End with
  `Signed-off-by: Michael Froh <m_froh@apple.com>`.
- **Several sessions share one checkout.** Never move HEAD: no `git checkout`, no `git checkout -b`. To put work
  on a branch, build the commit with a temporary index (`GIT_INDEX_FILE`, `read-tree`, `write-tree`,
  `commit-tree`, `git branch`). That commit is unsigned, so say so.
- **Verify, don't assume.** Check each new regression test by reverting the fix and watching it fail. Check
  claims about OpenSearch against `../OpenSearch` source, including claims from the data plane's notes. Where
  numbers can be measured, measure them and record them in the doc (see `DESIGN.md` §12.1).
- **This is headed for open source.** No company-internal or vendor product names in code, docs or commit
  messages. The sign-off address is the only exception.
- **`DESIGN.md` is mostly appended to.** When a section is overtaken, add a short "superseded by §N" note to it
  rather than rewriting it (see §26 → §27). Section numbers are cited across repositories.
- **Docker for integration tests.** Testcontainers finds the daemon through `docker context inspect`, and
  `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` must stay `/var/run/docker.sock`. The reason is in `WORKING_NOTES.md`;
  the wrong setting looks more correct.

## Where things stand (2026-09-29)

**Last landed:** primary terms and promotion. `DESIGN.md` §27–28, finding 11, and the most recent commits:
`contract_features`, `primary_terms` in every assignment, the per-index counter, promotion of a
document-replication replica when its primary is lost (`Promoter`), withdrawal of a returning old primary, and
the `PRIMARY_CONFLICT` finding. The data plane has implemented its half.

**Open threads, roughly in priority order:**

1. **The data plane's conformance adapter must fill `ContractNode.Understood.primaryTerms`.** Two checks fail on
   its side until it does, deliberately. Handed over in finding 11's status block.
2. **Somewhere to record that a drain or dual-run is intended.** Open question 11 (announced deployments) and
   §28.9. It blocks three things: intentional demotion (a drain), the rollback dual-run (§27.4), and exempting
   an intended dual-run from `PRIMARY_CONFLICT`. This is the next design task.
3. **The rest of §28:** `retain_remote_history` and `abandon_remote_terms`, the scoped metadata lock (§27.5),
   and the upstream search-replica pin (§27.3). None of it is built.
4. **The in-sync set is not observed.** `Promoter` promotes any started replica, where OpenSearch promotes only
   an in-sync one.
5. **The status documents the data plane reads have no shared check.** `IndexStatus` and `NodeStatus` need the
   same kind of reading-side conformance check `readAssignment` has. Both cross-repo failures so far were a
   format documented correctly and read wrongly.
6. **Two projections still read the configured copy count rather than the authorised one:** `Differ` and
   `StatusComputation` (§15.2).
7. **Long-standing:** R5.3 snapshot metadata, cross-pool migration, the dedicated-pool ceiling, rollover, an
   ordinary replica of a segment-replication index (open question 10), and the heat signal (proposed in
   `NODE_CONTRACT.md`).

## After the move to a single repository

This checkout expected its siblings beside it: `../opensearch-kv-store`, `../opensearch-data-plane`,
`../opensearch-control-plane`, `../OpenSearch`, `../cluster-etcd`. The composite build
(`settings.gradle.kts`, `includeBuild`), `tools/jbuild.sh` (its `KV_STORE` default) and many cross-references in
the docs use those paths. The session doing the move may already have fixed some. **Check before assuming a path.**
