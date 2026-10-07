# Findings from the control plane

Items the control plane (`../control-plane`) needs from this component, carried here by the user, in the form this
directory's `FINDINGS_FROM_THE_DATA_PLANE.md` uses: what happened, why, where the code is, what a change must be
careful about, and the tests worth having. Reply inline.

---

## 1. `status/cluster` needs a heartbeat, because a projection written only on change cannot show that it is stale

> **Closed.** Done as asked, with `H` = 10 s.
>
> - `StatusProjector` rewrites `tenant/<t>/status/cluster` when its content changes **or** once 10 s have passed
>   since it last wrote it, timed on the observation's clock, so at most one pass late. Every other projection stays
>   write-on-change. The decision is per key, from the time of the last write beside the digest, as you suggested.
> - **`heartbeat_ms`** is a new field in the document, so `T` can be derived rather than hardcoded. Your arithmetic
>   holds with the defaults: 10 s + the 15 s lease TTL + a pass, so 45 s is a safe `T`. The TTL is not in the tenant's
>   documents; it is in `supervisor/status/members/<id>` as `lease_ttl_millis` if you ever want it.
> - **It is now written last.** It used to be written before `status/awareness` and before gone index and node
>   documents were removed, so it did not quite vouch for the whole pass. Now it does, and the class says to keep it
>   that way.
> - Read-only mode and failed passes write nothing, so both read as stale; an absent document means not yet
>   projected, and nothing deletes it deliberately. `SUPERVISOR_KEYS.md` states the guarantee and these cases under
>   `status/cluster`, and corrects "can be rewritten every pass".
> - **Cost**, as you worked out: 0.1 writes/s per tenant. `../kv-store/BACKENDS.md` gives the supervisor's load as
>   `1.5 + P` writes/s, which now wants `+ 0.1 × tenants`; that document is the store's, so it is left for that
>   session.
>
> Tests: an unchanged `status/cluster` is rewritten after 10 s and not at 8 s, with a newer `observed_at` and
> the same content; a minute of quiet passes writes it six times and the index and node documents not at all;
> within a pass it is the last write, after a gone index's document is removed; read-only still writes nothing.
> All but the read-only test fail against the old projector.

### What the control plane is doing

The data plane now refuses `cluster.health` and `cluster.state` for a tenant and hands both to the control plane
(`../control-plane/FINDINGS_FROM_THE_DATA_PLANE.md`, item 1). The control plane answers them from the projections
in §1 of `SUPERVISOR_KEYS.md`, serving this service's figures as they are rather than recomputing them.

That works only while there is a supervisor behind the figures. When there is none, upstream has a precise
answer, and the control plane means to give it: a `cluster.health` request waits its `timeout` (30 s by default),
then fails with `503 cluster_manager_not_discovered_exception`, and with `local=true` it answers red with
`discovered_cluster_manager: false`. **Serving the last figures as current would be worse:** a tenant whose
supervisor died with every index green would keep reading green while nothing was managing its pool.

### Why the projections cannot say so today

`observed_at` and `observed_pass` are documented as what "make staleness visible" (`SUPERVISOR_KEYS.md` §1, and
the index projection's notes). They do not, because `StatusProjector` writes a projection **only when its content,
minus those two fields, has changed** (`supervisor-core/.../core/StatusProjector.java`, `write` and
`withoutTimestamps`). That is the right call for the per-index documents, for the write-amplification reason the
class gives. But it applies to `status/cluster` too, so on a quiet, healthy tenant `observed_at` stops moving. A
reader then sees the same thing whether the supervisor is healthy and nothing has changed, or the supervisor has
been gone for an hour.

`SUPERVISOR_KEYS.md` says `status/cluster` "can be rewritten every pass". That is true of its size, not of what the
code does, and it is the sentence a reader would rely on.

### What is asked

**Rewrite `tenant/<t>/status/cluster` at least every `H` even when its content is unchanged**, with `observed_at`
and `observed_pass` set to the current pass. Everything else stays write-on-change.

The control plane would then treat `status/cluster` as stale when `now − observed_at > T`, and answer as upstream
does with no cluster manager. It needs `H` stated as a guarantee to derive `T`:

| | value | why |
|---|---|---|
| `H`, heartbeat | 10 s, every fifth pass | proposed; this side's to choose |
| worst legitimate age | `H` + lease TTL + one pass = 10 + 15 + 2 = **27 s** | a supervisor dies just after a heartbeat; its pool is claimed after the TTL (`DESIGN.md`, "Failover"); the new owner's first pass writes everything, since its digest map starts empty |
| `T`, control plane threshold | **45 s** | the worst legitimate age plus margin for clock skew between the two processes, which compare wall clocks |

So a failover does not answer 503; a supervisor gone for longer than a failover takes does.

**Cost.** One write of a ~300-byte document per tenant per `H`. At 10 s that is a tenth of a write per second per
tenant, against today's budget of `1.5 + P` writes/s per supervisor (`../kv-store/BACKENDS.md`). A supervisor owning
pools with a thousand tenants adds 100 writes/s, which is the number to weigh before choosing `H`. Per-index and
per-node documents gain nothing.

### What a change should be careful about

- **The heartbeat vouches for the whole pass.** `project()` writes the index and node documents before
  `status/cluster`, so a fresh `status/cluster` means everything else this pass computed has been written too. The
  control plane will rely on that order and read the cluster document's freshness as the tenant's. Keep it, and say
  so in the class.
- **Only the lease holder writes it**, which the current placement inside `stillHoldsLease` already ensures. A pass
  that throws publishes nothing, so a pool whose passes keep failing goes stale, which is the right answer: nothing
  is managing it.
- **Read-only mode publishes no projections**, so under a read-only supervisor every tenant reads as stale and
  `cluster.health` answers 503. That is consistent with the class's stated choice, but worth a line where the
  control plane can find it.
- **A tenant no supervisor has projected yet has no `status/cluster` at all**: a new tenant, or one in an unbound
  pool. The control plane will treat absent the same as stale. Nothing is needed here, but if this side ever
  deletes `status/cluster` deliberately, the control plane needs to know.
- **`H` and the lease TTL are configuration.** If either changes, `T` has to follow, so publishing `H` somewhere a
  reader can see it, such as a field in `status/cluster` (`heartbeat_ms`), would let the control plane derive `T`
  rather than hardcode it. Optional; a stated default is enough to start.
- **The digest check must not be what skips the heartbeat.** The simplest correct shape is to keep, per key, the
  time of the last write alongside the digest, and write `status/cluster` when the digest differs **or** the last
  write is older than `H`.

### Tests worth having

- an unchanged `status/cluster` is rewritten once `H` has passed, with a newer `observed_at`, and not before;
- an unchanged per-index document is still not rewritten, however long passes;
- the write-count test's budget accounts for one heartbeat per tenant per `H`;
- within one pass, `status/cluster` is written after every index and node document;
- a read-only supervisor writes no `status/cluster`.

---

## Where these came from

Item 1 came from implementing `cluster.health` in the control plane. Checking how to detect a stale projection
meant reading `StatusProjector`, which showed that `observed_at` stops moving whenever nothing changes.
