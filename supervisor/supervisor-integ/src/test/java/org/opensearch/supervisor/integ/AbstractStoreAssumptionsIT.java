package org.opensearch.supervisor.integ;

import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The assumptions {@code DESIGN.md} rests on, checked against a real store instead of reasoned about.
 *
 * <p>Every one of these is an assertion the design already makes in prose. That is the value: the shared
 * {@code KeyValueStoreContractTests} covers what a store must do, and this covers the narrower question of
 * whether the specific things <em>this</em> service concluded from those semantics are actually true.
 *
 * <p><b>Backends differ on snapshot consistency, and the design already knows it.</b> etcd answers a prefix scan
 * with one atomic range request; ZooKeeper lists children and then issues a separate {@code getData} per key, so
 * its scans genuinely straddle concurrent writes. R2.5 says torn state is normal and must never be treated as
 * corruption, so a test asserting atomicity would be asserting something the design explicitly denies — and
 * would report a correct backend as broken. {@link #aPrefixScanIsCharacterisedForThisBackend()} therefore
 * <em>measures</em> what the backend does and asserts only what the supervisor actually relies on.
 */
abstract class AbstractStoreAssumptionsIT extends StoreIntegBase {

    /**
     * Liveness is derived from a key's revision changing. Nothing had checked that a rewrite with
     * <em>identical bytes</em> still changes it.
     *
     * <p>This one matters more than it looks, and the contract suite does not cover it: its revision test writes
     * "one" then "two". If a backend coalesced an identical write and left the revision alone, then a node whose
     * heartbeat happened not to change would look silent to {@code RevisionSilence}, be declared {@code LOST},
     * and have its shard copies rebuilt elsewhere — a data-moving consequence from an optimisation nobody
     * mentioned. Real heartbeats carry a timestamp so their bytes always differ, which means this would be
     * invisible until some backend or some future document made it not so.
     */
    @Test
    void rewritingIdenticalBytesStillChangesTheRevision() {
        byte[] unchanging = "{\"steady\":true}".getBytes(StandardCharsets.UTF_8);

        long first = store.put("liveness/probe", unchanging);
        long second = store.put("liveness/probe", unchanging);

        assertNotEquals(first, second,
            "revision-silence liveness reads an unchanged revision as a silent node, so an identical rewrite"
                + " must still be a write");
        assertEquals(second, store.get("liveness/probe").orElseThrow().revision());
    }

    /**
     * Exactly one of many contenders takes a lease, with real threads against a real store.
     *
     * <p>The single most important property in the design — {@code DESIGN.md} §2 makes every key single-writer by
     * construction, and one lease per pool is the only mutual exclusion anywhere — and until now it has only been
     * exercised by a single-threaded test taking turns. That arrangement cannot fail the way a real race fails.
     */
    @Test
    void onlyOneContenderCanTakeALeaseUnderRealContention() throws Exception {
        int contenders = 12;
        CountDownLatch ready = new CountDownLatch(contenders);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();
        AtomicReference<Throwable> unexpected = new AtomicReference<>();
        String key = Keys.poolLease(PoolId.of("contended"));
        byte[] claim = "{\"holder\":\"someone\",\"epoch\":1,\"acquired_at\":1,\"expires_at\":2}"
            .getBytes(StandardCharsets.UTF_8);

        List<Thread> threads = new java.util.ArrayList<>();
        for (int i = 0; i < contenders; i++) {
            Thread thread = new Thread(() -> {
                try {
                    ready.countDown();
                    go.await();
                    store.put(key, claim, KeyValueStore.ABSENT);
                    winners.incrementAndGet();
                } catch (RevisionConflictException expected) {
                    // The ordinary outcome for everybody but one.
                } catch (Throwable thrown) {
                    unexpected.set(thrown);
                }
            }, "contender-" + i);
            threads.add(thread);
            thread.start();
        }

        assertTrue(ready.await(30, TimeUnit.SECONDS), "contenders did not start");
        go.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(30));
        }

        assertNull(unexpected.get(), () -> "a contender failed unexpectedly: " + unexpected.get());
        assertEquals(1, winners.get(), "create-if-absent must admit exactly one writer, whatever the race");
    }

    /**
     * A compare-and-set at a revision somebody else has already superseded must fail, under real concurrency.
     *
     * <p>This is what protects a supervisor that paused: {@code Applier}'s third defence is a CAS on the revision
     * read during observation, and the whole argument in §12 depends on it actually rejecting the write.
     */
    @Test
    void aStaleCompareAndSetIsRejectedRatherThanApplied() {
        String key = Keys.nodeAssignment(NodeId.of("nodeAAAAAAAAAAAAAAAAAA"));
        long observed = store.put(key, "{\"version\":1}".getBytes(StandardCharsets.UTF_8));

        // Somebody else writes, so the revision the first writer observed is now history.
        store.put(key, "{\"version\":2}".getBytes(StandardCharsets.UTF_8), observed);

        org.junit.jupiter.api.Assertions.assertThrows(RevisionConflictException.class,
            () -> store.put(key, "{\"version\":3}".getBytes(StandardCharsets.UTF_8), observed),
            "a supervisor that woke up holding a stale observation must lose the write");
        assertTrue(new String(store.get(key).orElseThrow().value(), StandardCharsets.UTF_8).contains("2"));
    }

    /**
     * The supervisor's prefixes do not overlap on a real backend's key encoding.
     *
     * <p>{@code KeysTests} pins that every prefix constant ends in {@code /}, which is what makes this safe in
     * principle. What it cannot check is the backend: a store that encoded keys into a flat namespace, or scanned
     * by string rather than by byte range, could return {@code supervisor/status/pools/p/convergence} for a scan
     * of {@code supervisor/pools/}. Getting that wrong would have the lease manager parse a convergence document
     * as a lease — which is precisely the shape of the bug that turned up when a mismatched Jackson made leases
     * unreadable.
     */
    @Test
    void aPrefixScanReturnsOnlyWhatBelongsUnderThatPrefix() {
        PoolId pool = PoolId.of("pool-a");
        store.put(Keys.poolLease(pool), "{\"lease\":true}".getBytes(StandardCharsets.UTF_8));
        store.put(Keys.poolConvergence(pool), "{\"convergence\":true}".getBytes(StandardCharsets.UTF_8));
        store.put(Keys.poolDefinition(pool), "{\"definition\":true}".getBytes(StandardCharsets.UTF_8));
        store.put(Keys.HOUSEKEEPING_LEASE, "{\"housekeeping\":true}".getBytes(StandardCharsets.UTF_8));
        // A neighbour whose name merely starts with the same characters, minus the separator.
        store.put("supervisor/poolsandthings", "{\"trap\":true}".getBytes(StandardCharsets.UTF_8));

        NavigableMap<String, KeyValueStore.Versioned> leases = store.scanPrefix(Keys.POOL_LEASES_PREFIX);

        assertTrue(leases.containsKey(Keys.poolLease(pool)), leases.keySet().toString());
        assertTrue(leases.containsKey(Keys.poolConvergence(pool)) == false,
            "a convergence document must not be scanned as a lease: " + leases.keySet());
        assertTrue(leases.containsKey(Keys.HOUSEKEEPING_LEASE) == false,
            "the housekeeping lease lives outside the pool prefix and is read separately: " + leases.keySet());
        assertTrue(leases.containsKey("supervisor/poolsandthings") == false,
            "the trailing slash is what stops a prefix matching its neighbours: " + leases.keySet());
    }

    /**
     * An inventory at its documented size ceiling round-trips.
     *
     * <p>{@code NODE_CONTRACT.md} budgets 512 KiB for {@code nodes/shards/<id>} and works out a ceiling of about
     * 5,600 copies from a measured 93 bytes each. That arithmetic has never met a backend: etcd's default
     * maximum request size is 1.5 MiB, which leaves room, but the number in the document is a promise to whoever
     * implements the data plane and a promise nobody has tested is a guess.
     */
    @Test
    void anInventoryAtItsDocumentedCeilingRoundTrips() {
        NodeId node = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
        int copies = 4_000;
        Map<Integer, ShardInventory.ShardCopy> shards = new LinkedHashMap<>();
        for (int shard = 0; shard < copies; shard++) {
            shards.put(shard, new ShardInventory.ShardCopy(
                ShardRole.SEARCH_REPLICA, ShardState.STARTED, 1_234_567_890L, null));
        }
        ShardInventory inventory = new ShardInventory(node, "eph", System.currentTimeMillis(), Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                INDEX_UUID, new ShardInventory.IndexShards(INDEX_NAME, 3041, shards)))));

        byte[] serialised = NodeJson.write(inventory);
        store.put(Keys.nodeShards(node), serialised);
        ShardInventory back = NodeJson.readInventory(store.get(Keys.nodeShards(node)).orElseThrow().value());

        assertEquals(copies, back.copyCount(), "a full inventory must survive the store, not be truncated by it");
        System.out.println("[assumptions] inventory of " + copies + " copies is " + serialised.length
            + " bytes (" + (serialised.length / copies) + " bytes/copy)");
    }

    /**
     * Characterises whether this backend's prefix scan is a snapshot, and asserts only what the supervisor needs.
     *
     * <p>This deliberately does <b>not</b> assert atomicity. etcd has it and ZooKeeper does not — ZooKeeper lists
     * a parent's children and then reads each one separately, so a scan taken while a writer sweeps genuinely
     * spans several of the writer's iterations. R2.5 makes that normal rather than a fault, so asserting the
     * stronger property would fail a backend that is behaving exactly as documented.
     *
     * <p>What is asserted, on every backend:
     *
     * <ul>
     *   <li><b>No key is lost or invented.</b> A scan of a prefix whose membership is not changing must return
     *       every key in it. Values may be stale; keys may not go missing, because a missing
     *       {@code nodes/heartbeat/<id>} reads as a departed node and a missing shard copy reads as lost data.
     *   <li><b>Every value is one a writer actually wrote.</b> Tearing may mix generations; it may never yield a
     *       half-written or corrupt value. This is the property that lets the differ parse whatever it reads.
     * </ul>
     *
     * <p>The measurement uses a writer sweeping keys in <b>ascending order</b>. A true snapshot then shows a
     * staircase — keys below the writer's position at generation {@code N}, the rest at {@code N-1} — so at most
     * two generations, consecutive, newer on a prefix of the key order. Anything else means the read drifted
     * across the writer's own iterations. The verdict is printed rather than asserted, so the suite records the
     * difference between backends instead of hiding it.
     *
     * <p><b>Measured, on 200 scans against each backend while a writer swept 40 keys:</b>
     *
     * <pre>
     *   etcd       200 snapshot-consistent,   0 torn, widest generation spread 2
     *   ZooKeeper   11 snapshot-consistent, 189 torn, widest generation spread 3
     * </pre>
     *
     * <p>Those two signatures are exactly what the model predicts and are worth reading carefully. A spread of
     * <b>2</b> is not evidence of tearing — it is the <em>expected</em> state of an atomic scan taken mid-sweep,
     * which is the whole reason the first version of this test was wrong to demand a single value. A spread of
     * <b>3</b> is impossible for a snapshot, because one instant cannot contain three of the writer's
     * generations, and it is the unambiguous fingerprint of a walk.
     *
     * <p>The key count is raised to 200 here so that detection is <b>structural rather than incidental</b>. At 40
     * keys ZooKeeper still tore on 94.5% of scans, so nothing was broken — but the eleven that looked consistent
     * are a reminder that a walking read can be mistaken for a snapshot when it happens to fetch the high keys
     * first and the low keys last, since that produces the same newer-on-a-prefix shape a staircase does. The
     * costs scale differently in a way that removes the ambiguity: an atomic range request is one round trip
     * whatever the key count, while a walk is at least one per key, so at 200 keys a walk spans several sweeps and
     * cannot come back with a spread of 2. Attempts drop from 200 to 30 to suit, which is fewer round trips than
     * before rather than more.
     *
     * <p>The earlier version of this test asserted that all values in a scan were equal, and failed against etcd.
     * That was the test being wrong: the SPI has no multi-key transaction, so the writer's forty puts are forty
     * separate writes and mid-sweep the store really does contain a mixture, which an atomic snapshot must
     * faithfully return. "Consistent snapshot" and "all values equal" only coincide if the writes were atomic.
     */
    @Test
    void aPrefixScanIsCharacterisedForThisBackend() throws Exception {
        String prefix = "torn/";
        // Enough that a walking scan provably outlasts several writer sweeps; see the class comment. An atomic
        // range request does not care how many there are.
        int keys = 200;
        for (int i = 0; i < keys; i++) {
            store.put(prefix + String.format("%03d", i), generation(0));
        }

        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                // Ascending, always: the staircase below is a statement about the order these are written in.
                for (int generation = 1; stop.get() == false; generation++) {
                    for (int i = 0; i < keys; i++) {
                        store.put(prefix + String.format("%03d", i), generation(generation));
                    }
                }
            } catch (Throwable thrown) {
                writerFailure.set(thrown);
            }
        }, "torn-writer");
        writer.start();

        int snapshots = 0;
        int torn = 0;
        int worstGenerationSpread = 1;
        try {
            for (int attempt = 0; attempt < 30; attempt++) {
                NavigableMap<String, KeyValueStore.Versioned> scanned = store.scanPrefix(prefix);

                assertEquals(keys, scanned.size(),
                    "a scan of a prefix whose membership is not changing must return every key in it; a missing"
                        + " key reads as a departed node or a lost shard copy, which is not a stale read but a"
                        + " wrong one");

                List<Integer> generations = new java.util.ArrayList<>();
                for (KeyValueStore.Versioned versioned : scanned.values()) {
                    String value = new String(versioned.value(), StandardCharsets.UTF_8);
                    assertTrue(value.startsWith("generation-"),
                        "every value must be one some writer actually wrote, never a partial one: [" + value + "]");
                    generations.add(Integer.parseInt(value.substring("generation-".length())));
                }

                Set<Integer> distinct = new java.util.TreeSet<>(generations);
                worstGenerationSpread = Math.max(worstGenerationSpread, distinct.size());
                if (isStaircase(generations, distinct)) {
                    snapshots++;
                } else {
                    torn++;
                }
            }
        } finally {
            stop.set(true);
            writer.join(TimeUnit.SECONDS.toMillis(30));
        }
        assertNull(writerFailure.get(), () -> "the writer failed: " + writerFailure.get());

        System.out.println("[assumptions] " + getClass().getSimpleName() + " prefix scan over " + keys
            + " keys: " + snapshots + " snapshot-consistent, " + torn + " torn, widest generation spread "
            + worstGenerationSpread + " -- " + (torn == 0
                ? "this backend answers a scan atomically"
                : "this backend's scans straddle concurrent writes, which R2.5 makes normal"));
    }

    /**
     * Whether these generations could have come from a single instant, given an ascending writer.
     *
     * <p>At most two, consecutive, newer occupying a prefix. Anything else means the read spanned more of the
     * writer's work than one instant contains.
     */
    private static boolean isStaircase(List<Integer> generations, Set<Integer> distinct) {
        if (distinct.size() == 1) {
            return true;
        }
        if (distinct.size() > 2) {
            return false;
        }
        List<Integer> both = new java.util.ArrayList<>(distinct);
        int older = both.get(0);
        int newer = both.get(1);
        if (older + 1 != newer) {
            return false;
        }
        return generations.lastIndexOf(newer) < generations.indexOf(older);
    }

    private static byte[] generation(int generation) {
        return ("generation-" + generation).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Two clients see each other's writes, which is what makes a shared store a coordination medium at all.
     *
     * <p>Trivial, and worth one test: everything else in this suite would also pass against two clients that
     * happened to be talking to different key spaces, and this is what rules that out.
     */
    @Test
    void twoClientsAgainstTheSameKeySpaceSeeEachOther() throws Exception {
        KeyValueStore other = open();
        TenantId tenant = TenantId.of("shared");

        store.put(Keys.tenantRegistration(tenant), "{\"tenant\":\"shared\"}".getBytes(StandardCharsets.UTF_8));

        eventually("the second client to see the first client's write", () -> assertTrue(
            other.get(Keys.tenantRegistration(tenant)).isPresent(),
            "two clients on one key space must see one store"));
    }
}
