package org.opensearch.supervisor.integ;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
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
 * <p>One caveat stated up front, because overclaiming here would be worse than not testing. etcd serves a prefix
 * scan as <b>one atomic range request</b>, so it is the easy case for the torn-read concern in §12 — the
 * assertions below will pass on etcd whether or not the supervisor handles a torn read correctly. They are worth
 * having anyway: they pin the assumption where a future backend would break it, and ZooKeeper, which walks the
 * subtree with separate calls, is the backend that would. Running this suite against ZooKeeper is the follow-on.
 */
@EnabledIf("org.opensearch.supervisor.integ.EtcdTestServer#available")
class EtcdStoreAssumptionsIT extends EtcdIntegBase {

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
     * A prefix scan reflects one point in time, checked by an invariant a torn read cannot satisfy.
     *
     * <p>The first version of this test was simply wrong, and the mistake is worth keeping because it is an easy
     * one to make again. It asserted that every key returned by a scan carried the same value while a writer
     * rewrote all of them — and failed against etcd, which looks like a finding and is not. The {@code
     * KeyValueStore} SPI has no multi-key transaction, so the writer updates forty keys with forty separate
     * puts; mid-sweep the store genuinely <em>contains</em> a mixture, and a perfectly atomic snapshot must
     * therefore return one. "Consistent snapshot" and "all values equal" only coincide if the writes were
     * atomic, which these cannot be.
     *
     * <p>What a snapshot does guarantee, given a writer that sweeps the keys in <b>ascending order</b>, is a
     * staircase: keys below the writer's position hold generation {@code N}, keys above it hold {@code N-1}, and
     * nothing holds anything else. So a correct scan shows <b>at most two distinct generations, differing by
     * exactly one, with the newer occupying a prefix of the key order</b>.
     *
     * <p>A reader that walked the subtree with separate calls, as ZooKeeper must, would drift across the
     * writer's own iterations and produce three or more generations, or two that are not adjacent, or a newer
     * value sitting <em>above</em> an older one. Each of those fails an assertion below. That is the shape of the
     * torn read {@code DESIGN.md} §12 reasons about, and this is what it would actually look like.
     *
     * <p>etcd serves a prefix scan as one atomic range request, so it satisfies this by construction — the test
     * earns its place by being the executable form of the assumption, ready for the backend that does not.
     */
    @Test
    void aPrefixScanReflectsOnePointInTime() throws Exception {
        String prefix = "torn/";
        int keys = 40;
        for (int i = 0; i < keys; i++) {
            store.put(prefix + String.format("%03d", i), generation(0));
        }

        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                // Ascending, always. The invariant below is a statement about the order these are written in;
                // a writer that shuffled would make a consistent snapshot indistinguishable from a torn one.
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

        try {
            for (int attempt = 0; attempt < 200; attempt++) {
                NavigableMap<String, KeyValueStore.Versioned> scanned = store.scanPrefix(prefix);
                assertEquals(keys, scanned.size(), "the scan lost or gained keys mid-flight");

                List<Integer> generations = scanned.values().stream()
                    .map(versioned -> Integer.parseInt(
                        new String(versioned.value(), StandardCharsets.UTF_8).substring("generation-".length())))
                    .toList();

                Set<Integer> distinct = new java.util.TreeSet<>(generations);
                assertTrue(distinct.size() <= 2,
                    () -> "a snapshot can straddle at most one writer sweep, so at most two generations may"
                        + " appear at once; saw " + distinct + ", which means the scan drifted across several"
                        + " iterations of the writer (DESIGN.md §12)");

                if (distinct.size() == 2) {
                    int older = distinct.iterator().next();
                    int newer = new java.util.ArrayList<>(distinct).get(1);
                    assertEquals(older + 1, newer,
                        () -> "two generations at once must be consecutive; " + distinct + " means the scan"
                            + " spanned at least one whole sweep");

                    // The newer generation must occupy a prefix, because that is where the ascending writer has
                    // already been. A newer value above an older one is a reader that went backwards in time.
                    int firstOlder = generations.indexOf(older);
                    int lastNewer = generations.lastIndexOf(newer);
                    assertTrue(lastNewer < firstOlder,
                        () -> "generation " + newer + " appears at index " + lastNewer + ", after generation "
                            + older + " at index " + firstOlder + "; an ascending writer cannot produce that,"
                            + " so the scan is not one point in time: " + generations);
                }
            }
        } finally {
            stop.set(true);
            writer.join(TimeUnit.SECONDS.toMillis(30));
        }
        assertNull(writerFailure.get(), () -> "the writer failed: " + writerFailure.get());
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
