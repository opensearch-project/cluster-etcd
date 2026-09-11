package org.opensearch.supervisor.integ;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStores;
import org.opensearch.supervisor.core.Balancer;
import org.opensearch.supervisor.core.Supervisor;
import org.opensearch.supervisor.fakenode.FakeNode;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.plan.Planner;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.status.Replication;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shared setup: a real etcd, a fresh key space per test, and a real clock.
 *
 * <p><b>There is no {@code MutableClock} here, and that is the point.</b> Every other test in this repository
 * advances time by calling a method, which is what makes them fast and deterministic — and it means a lease TTL,
 * a staleness threshold and a revision-silence window have never once been measured against a clock that moves
 * on its own. Time here is wall-clock, so the numbers have to be small enough for a test to wait out and large
 * enough not to fire spuriously. That tension is itself worth exercising.
 */
/*
 * Note: the @EnabledIf that gates these on Docker lives on each concrete subclass, not here. EnabledIf is not
 * declared @Inherited, and while JUnit's own annotation lookup does walk the class hierarchy, "probably
 * inherited" is a poor thing to rest a skip on: if it were not found, these would *fail* without Docker instead
 * of skipping, which is the difference between a red build and an honest one on any machine without it.
 */
abstract class EtcdIntegBase {

    static final PoolId POOL_A = PoolId.of("pool-a");
    static final TenantId ACME = TenantId.of("acme");
    static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";
    static final String INDEX_NAME = "my-index";

    /**
     * Short, because a test has to wait it out in real time, and not shorter, because a lease that expires
     * faster than a pass takes would hand pools around continuously and prove nothing.
     */
    static final Duration LEASE_TTL = Duration.ofSeconds(4);

    /** How long an assertion may keep retrying before it is a failure rather than slowness. */
    static final Duration PATIENCE = Duration.ofSeconds(60);

    private static final AtomicLong NAMESPACES = new AtomicLong();

    KeyValueStore store;
    String storeUri;
    private final List<AutoCloseable> toClose = new ArrayList<>();

    @BeforeEach
    void openStore() throws Exception {
        // A key space per test. etcd keeps everything written by every test in one cluster otherwise, and a
        // leftover pool lease from a previous test is exactly the kind of state that makes an integration suite
        // pass or fail depending on order.
        storeUri = EtcdTestServer.storeUri(
            "supervisor-integ-" + System.nanoTime() + "-" + NAMESPACES.incrementAndGet());
        store = open();
    }

    @AfterEach
    void closeStores() {
        for (AutoCloseable closeable : toClose) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // A store that will not close cannot fail a test that has already made its assertions.
            }
        }
        toClose.clear();
    }

    /**
     * Another client against the same key space.
     *
     * <p>Used where a test needs two supervisors that are genuinely independent. Sharing one client would make
     * them share a connection and a watch, which is not what two processes do.
     */
    KeyValueStore open() throws Exception {
        KeyValueStore opened = KeyValueStores.open(storeUri);
        toClose.add(opened);
        return opened;
    }

    // ---------------------------------------------------------------- desired state

    void definePool(PoolId pool) {
        store.put(Keys.poolDefinition(pool),
            new PoolDefinition(pool, PoolKind.SHARED, false, null, null, List.of()).toBytes());
    }

    void registerTenant(TenantId tenant, PoolId pool) {
        store.put(Keys.poolBinding(tenant), PoolBinding.to(pool).toBytes());
        store.put(Keys.tenantRegistration(tenant),
            ("{\"tenant\":\"" + tenant.value() + "\"}").getBytes(StandardCharsets.UTF_8));
    }

    void createIndex(TenantId tenant, String name, String uuid, int shards, int replicas) {
        store.put(Keys.indicesPrefix(tenant) + name, IndexConfig.toDocument(
            name, uuid, true, IndexConfig.settingsOf(shards, replicas, Replication.SEGMENT)
        ));
    }

    // ---------------------------------------------------------------- actors

    Supervisor supervisor(String id, KeyValueStore against) {
        return new Supervisor(against, Clock.systemUTC(), id, Set.of(), 1.0, LEASE_TTL,
            Balancer.Config.defaults(), new Planner.Policy(200, 5, 2), false);
    }

    Supervisor supervisor(String id) {
        return supervisor(id, store);
    }

    /**
     * A data node in a pool.
     *
     * <p>Distinct addresses per node, because {@code SameShardDecider} treats one address as one failure domain
     * and a fixture that shared them would make every second copy unplaceable — which reads as a placement bug
     * rather than as the fixture mistake it is.
     */
    FakeNode node(NodeId id, PoolId pool, KeyValueStore against) {
        return new FakeNode(against, new Registration(
            id, "placeholder", id.value(), pool, EnumSet.of(NodeRole.DATA),
            "10.0.0." + (Math.abs(id.value().hashCode()) % 250 + 1), 9300, "3.7.0",
            Map.of("zone", "z1"),
            // Fast heartbeats, so liveness has something to observe within a test's patience. The supervisor
            // derives its thresholds from this, floored, so the effective interval is a little over 3s.
            1000, 0L
        ), Clock.systemUTC());
    }

    FakeNode node(NodeId id, PoolId pool) {
        return node(id, pool, store);
    }

    // ---------------------------------------------------------------- waiting

    /**
     * Retries an assertion until it holds or patience runs out.
     *
     * <p>Necessary because nothing here can be stepped. The last failure is rethrown rather than a timeout
     * message, so a test that gives up says <em>what</em> was still wrong rather than only that it waited.
     */
    static void eventually(String what, Runnable assertion) {
        eventually(what, PATIENCE, assertion);
    }

    static void eventually(String what, Duration patience, Runnable assertion) {
        long deadline = System.nanoTime() + patience.toNanos();
        Throwable last = null;
        while (System.nanoTime() < deadline) {
            try {
                assertion.run();
                return;
            } catch (Throwable thrown) {
                last = thrown;
                sleep(Duration.ofMillis(200));
            }
        }
        fail("gave up waiting for " + what + " after " + patience.toSeconds() + "s; last failure: "
            + (last == null ? "none" : last.toString()), last);
    }

    static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting", e);
        }
    }

    /**
     * Drives a supervisor and some nodes for a while, in this thread.
     *
     * <p>Sequential on purpose where concurrency is not what is under test: it keeps the failure comprehensible
     * while still going through a real store and a real clock, which is what these tests are for. The lease
     * tests do use real threads, because there the interleaving <em>is</em> the subject.
     */
    void pump(Supervisor supervisor, List<FakeNode> nodes, Duration forHowLong) {
        long deadline = System.nanoTime() + forHowLong.toNanos();
        while (System.nanoTime() < deadline) {
            supervisor.tick();
            for (FakeNode node : nodes) {
                node.tick();
            }
            sleep(Duration.ofMillis(250));
        }
    }
}
