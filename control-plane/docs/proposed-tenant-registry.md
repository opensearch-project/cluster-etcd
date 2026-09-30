# Proposed: a tenant registry key

**Status:** **ACCEPTED and implemented**, 2026-09-09. Kept for the reasoning; the code is the authority.
Originally a proposal from the supervisor (management plane) side.

Implemented as proposed, with two deltas and one correction:

- **The first-write hook is its own decorator**, `RegisteringKeyValueStore`, rather than a field on
  `TenantScopedKeyValueStore`. That class documents itself as the whole tenancy mechanism — a view through
  which cross-tenant access "is not something a handler can get wrong" — and a hook on it that writes
  *outside* the tenant prefix would quietly weaken the claim. `OperationContextFactory` composes the two,
  so the registration is visible where the wiring is.
- **A transient store failure is retried** on the tenant's next write rather than being remembered as
  registered. The sketch added to the `known` set on the generic failure path, which would have suppressed
  retries for the life of the process.
- **The proposal missed a test it breaks.** `TenantIsolationTests.keysAreLaidOutUnderTheTenantPrefix`
  asserts the exact contents of `scanPrefix("")`. It now includes `tenants/acme`, which is a feature: that
  assertion is what keeps this the *only* root key.
**Requested by:** `../supervisor` — see its `SUPERVISOR_KEYS.md` §6 and `DESIGN.md` §3.
**Size:** one new key, one call site, no new dependency, no API change.

## The problem

The supervisor reconciles every tenant, so it has to know which tenants exist. **The store cannot tell it.**

`KeyValueStore` has `get` and `scanPrefix` and no list-children or delimiter operation, and `scanPrefix` is
specified over key prefixes rather than path boundaries. So the only way to discover tenant ids today is
`scanPrefix("tenant/")`, which returns **every key of every tenant** — the entire store, on every pass, to
learn a list of names. For a deployment with ten thousand indices that is not a discovery mechanism, it is
a full table scan with the answer thrown away.

The alternative on the supervisor's side is a configured list of tenants, which is what it will do until
this exists. That is wrong the first time someone onboards a tenant without restarting the supervisor —
the tenant is served by the control plane, has indices, and is silently unmanaged: no shards placed, no
health, no task reaping, and nothing anywhere saying so.

## The proposal

One new key at the **root** of the store, outside any tenant prefix:

```
tenants/<tenant-id>     { "tenant": "acme", "registered_at": 1788969977933 }
```

Written by the control plane the first time a tenant is seen to have state. Never deleted by the control
plane. The supervisor then discovers tenants with one small scan:

```java
store.scanPrefix("tenants/");
```

It has to be at the root. A marker inside the tenant prefix — `tenant/<id>/registered` — would need
`scanPrefix("tenant/")` to find, which is the problem being solved.

**The control plane already writes exactly this shape of key.** `ingest/metadata-initialized`,
`search/metadata-initialized` and `snapshot/repositories-initialized` are markers whose existence is the
information. This is the same pattern with the tenant id in the key, and it relies on the same store
guarantee — that an empty or trivial value stays distinguishable from an absent key, which
`KeyValueStoreContractTests.emptyValuesRoundTrip` already pins down.

An empty value would do. A two-field document is proposed instead because `registered_at` costs nothing and
answers "when did this tenant first appear", which is otherwise unrecoverable.

## Where it goes

`OperationContextFactory` is the only place in the control plane that holds both the **root** store and a
resolved `TenantId`:

```java
public Optional<OperationContext> create(Map<String, String> headers) {
    return tenantResolver.resolve(headers)
        .map(tenant -> new OperationContext(tenant, new TenantScopedKeyValueStore(rootStore, tenant), clock));
}
```

Handlers cannot do it themselves: they see a `TenantScopedKeyValueStore` rooted at `tenant/<id>/`, so for
them a root key is not merely undiscovered, it is unreachable.

### Register on first write, not on first request

**Recommended:** register when the tenant first *writes* something, not when a request first names it.

`HeaderTenantResolver` in its unrestricted form accepts any well-formed `x-tenant-id` value. If registration
happens in `create(...)`, then anyone who can reach the service can grow the registry without limit by
sending requests with made-up tenant ids — 128 characters of `[A-Za-z0-9_.-]` each. Those tenants have no
state, but the supervisor would try to reconcile every one of them, and the registry only grows.

Registering on first write bounds the registry by tenants that actually have state, which is also the
honest definition: a tenant exists once something has been written for it. It matches the `*-initialized`
markers, which appear on first write rather than first read.

Mechanically, give `TenantScopedKeyValueStore` a callback invoked before its first mutating call:

```java
// TenantScopedKeyValueStore — one field, invoked at most once per instance
private final Runnable onFirstWrite;          // default: () -> {}
private final AtomicBoolean registered = new AtomicBoolean();

@Override
public long put(String key, byte[] value) {
    ensureRegistered();
    return delegate.put(scoped(key), value);
}
// ... same first line in put(key, value, expectedRevision), delete(key), delete(key, expectedRevision)

private void ensureRegistered() {
    if (registered.compareAndSet(false, true)) {
        onFirstWrite.run();
    }
}
```

and have `OperationContextFactory` supply it:

```java
.map(tenant -> new OperationContext(
        tenant,
        new TenantScopedKeyValueStore(rootStore, tenant, () -> registry.ensureRegistered(tenant)),
        clock))
```

**Simpler variant, if the callback is unwelcome:** register directly in `create(...)`. One less moving part,
and it gives up the bound on registry growth. Worth taking only if the deployment restricts tenants with
`HeaderTenantResolver.restrictedTo(...)`, where the permitted set already bounds it.

### The registration itself

```java
final class TenantRegistry {

    private final KeyValueStore rootStore;
    private final Clock clock;
    private final Set<TenantId> known = ConcurrentHashMap.newKeySet();

    void ensureRegistered(TenantId tenant) {
        if (known.contains(tenant)) {
            return;                                  // hot path: one set lookup, no store call
        }
        String key = "tenants/" + tenant.value();
        try {
            if (rootStore.get(key).isEmpty()) {
                byte[] value = ("{\"tenant\":\"" + tenant.value() + "\",\"registered_at\":"
                        + clock.millis() + "}").getBytes(StandardCharsets.UTF_8);
                rootStore.put(key, value, KeyValueStore.ABSENT);
            }
            known.add(tenant);
        } catch (RevisionConflictException e) {
            known.add(tenant);                       // another process won the race; that is success
        } catch (RuntimeException e) {
            LOGGER.warn("could not register tenant [{}]; supervisor discovery may lag", tenant, e);
        }
    }
}
```

Four properties, all of them deliberate:

- **`put(key, value, KeyValueStore.ABSENT)`** is create-if-absent. `ABSENT` is `0L` on the interface, and a
  losing racer gets `RevisionConflictException`, which is the correct outcome and not an error.
- **The per-process cache makes this free after the first write per tenant.** One set lookup on the hot
  path. Do not skip it: without it every write operation costs an extra `get`.
- **It is best-effort and must never fail the request.** If the registry write throws, log and carry on.
  This is a discovery hint for another service, exactly like the task records — the control plane's
  correctness does not depend on it, and neither does the supervisor's. A supervisor with a stale registry
  reconciles fewer tenants than it should and says so; a control plane that 500s because a marker could not
  be written is strictly worse.
- **Nothing deletes it.** A tenant whose keys are all gone may still be intended to exist, and the control
  plane has no way to know. Removal is an operator action. The supervisor's side of this is to raise a
  finding for a registered tenant with no state rather than to assume either answer.

## What the supervisor does with it

- `scanPrefix("tenants/")` each pass — small, and the values are only read on change.
- A registered tenant not bound to a node pool raises `TENANT_UNPOOLED`, so onboarding a tenant and
  forgetting to give it capacity is visible instead of silent. This is the failure the registry exists to
  make impossible, and it cannot be detected from a configured list.
- Until this exists, the supervisor falls back to configuration and reads the registry when present.

## Also needs updating

- **`KEY_LAYOUT.md`** — a row for `tenants/<tenant-id>` in the layout table, and a note that it is the one
  key the control plane writes *outside* the tenant prefix. That file is described as a contract that
  drifts, and this is a change to it.
- The README's note on the three `*-initialized` markers, if it enumerates root-level keys.

## Tests worth having

- Registration happens on first write and **not** on a read-only request.
- Two concurrent first-writes for the same tenant produce one key and no error, in the
  `RevisionConflictException` path specifically.
- A store that throws on `put` does not fail the operation.
- The per-process cache: N writes for the same tenant produce one `get` and one `put`.
- The registered value is well-formed JSON with a `registered_at` from the injected `Clock`, so the test
  need not be time-dependent.

## Open question for whoever picks this up

Should the registry also record something about the tenant, or is existence the whole payload? The
supervisor needs nothing else — pool binding is management-plane state and lives on its side, deliberately,
because the control plane has no concept of a node pool and should not acquire one. But if the control
plane has its own reason to keep per-tenant metadata at the root, this is the natural place, and it is
easier to widen the document now than after something depends on its shape.
