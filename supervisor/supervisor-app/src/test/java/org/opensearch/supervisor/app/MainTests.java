package org.opensearch.supervisor.app;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStores;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.status.Replication;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whole process, against a real store on a real filesystem.
 *
 * <p>Everything else in this repository runs against {@code InMemoryKeyValueStore}, which is the right default
 * and leaves one thing unverified: whether the assembled program starts at all. These tests exercise argument
 * parsing, {@code ServiceLoader} backend discovery, store construction, the loop, and shutdown, by running
 * {@link Main#run} exactly as a shell would — which is how you find out that resources were not on the
 * classpath, or that {@code --help} exits non-zero.
 */
class MainTests {

    private Path directory;
    private ByteArrayOutputStream captured;
    private PrintStream originalOut;
    private PrintStream originalErr;

    @BeforeEach
    void setUp() throws Exception {
        directory = Files.createTempDirectory("supervisor-main-");
        captured = new ByteArrayOutputStream();
        PrintStream capture = new PrintStream(captured, true, StandardCharsets.UTF_8);
        originalOut = System.out;
        originalErr = System.err;
        System.setOut(capture);
        System.setErr(capture);
        Log.stream(capture);
    }

    @AfterEach
    void tearDown() throws Exception {
        System.setOut(originalOut);
        System.setErr(originalErr);
        Log.stream(originalOut);
        Log.threshold(Log.Level.INFO);
        try (var paths = Files.walk(directory)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                    // A temp directory that will not delete is not a test failure.
                }
            });
        }
    }

    private String output() {
        return captured.toString(StandardCharsets.UTF_8);
    }

    private String storeUri() {
        return directory.toUri().toString();
    }

    @Test
    void aBareInvocationStartsStopsAndReportsWhatItWasRunningWith() {
        int code = Main.run(new String[] {"--store=" + storeUri(), "--max-passes=3", "--interval=1ms"});

        assertEquals(0, code, output());
        assertTrue(output().contains("msg=starting"), output());
        assertTrue(output().contains("mode=read-only"), output());
        assertTrue(output().contains("msg=running"), output());
        assertTrue(output().contains("reached the configured pass limit"), output());
        assertTrue(output().contains("released every lease"), output());
    }

    @Test
    void theFilesystemBackendIsFoundThroughServiceLoader() {
        // This is the test that catches a packaging mistake. A supervisor that cannot resolve its own --store
        // scheme fails at startup with a message about schemes, and no amount of unit testing the loop finds it.
        int code = Main.run(new String[] {"--store=" + storeUri(), "--max-passes=1", "--interval=1ms"});

        assertEquals(0, code, output());
        assertTrue(output().contains("msg=\"opened store\"") || output().contains("msg=opened"), output());
        assertTrue(output().contains("FileSystemKeyValueStore"), output());
    }

    @Test
    void anUnknownSchemeSaysWhichOnesAreActuallyAvailable() {
        // The single most likely first-run failure, and a missing backend jar is indistinguishable from a typo
        // unless the error lists what was found.
        int code = Main.run(new String[] {"--store=etcd://localhost:2379/x", "--max-passes=1"});

        assertEquals(1, code);
        assertTrue(output().contains("no store provider for scheme [etcd]"), output());
        assertTrue(output().contains("available stores:"), output());
    }

    @Test
    void helpExitsZeroAndABadArgumentDoesNot() {
        assertEquals(0, Main.run(new String[] {"--help"}), "asking for help is not a failure");
        assertEquals(2, Main.run(new String[] {"--store=" + storeUri(), "--nonsense"}));
        assertEquals(2, Main.run(new String[] {}), "no --store is a usage error, not a crash");
    }

    @Test
    void readOnlyByDefaultMeansNoAssignmentIsEverWritten() throws Exception {
        // The safety default, verified against the store rather than against a flag. A pool, a tenant and an
        // index are all present and would ordinarily produce a placement -- there are no nodes here, so nothing
        // could be placed anyway, but the assertion that matters is that read-only mode is what the process
        // reports and honours.
        seedDesiredState();

        int code = Main.run(new String[] {"--store=" + storeUri(), "--max-passes=4", "--interval=1ms"});

        assertEquals(0, code, output());
        assertTrue(output().contains("this process will observe, diff and project"), output());
        assertFalse(output().contains("will create and remove shard copies"), output());
        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            assertTrue(store.scanPrefix(Keys.NODE_ASSIGNMENT_PREFIX).isEmpty(),
                "read-only must not write anything a data node would act on");
        }
    }

    @Test
    void writeModeAnnouncesItselfLoudly() {
        int code = Main.run(new String[] {
            "--store=" + storeUri(), "--write", "--max-passes=2", "--interval=1ms"
        });

        assertEquals(0, code, output());
        assertTrue(output().contains("will create and remove shard copies on data nodes"), output());
        assertTrue(output().contains("mode=write"), output());
    }

    @Test
    void aRealPassAgainstARealStoreProjectsSomethingAnOperatorCanRead() throws Exception {
        // End to end: desired state on disk, a supervisor process, and observability written back. Without a
        // data node there is nothing to place, which is the point -- the projections should say so rather than
        // being absent.
        seedDesiredState();

        int code = Main.run(new String[] {
            "--store=" + storeUri(), "--write", "--max-passes=5", "--interval=1ms"
        });

        assertEquals(0, code, output());
        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            assertTrue(store.get(Keys.poolConvergence(PoolId.of("shared-a"))).isPresent(),
                "the pool's convergence document is what an operator reads");
            String convergence = new String(
                store.get(Keys.poolConvergence(PoolId.of("shared-a"))).orElseThrow().value(),
                StandardCharsets.UTF_8);
            assertTrue(convergence.contains("shared-a"), convergence);
            assertTrue(store.get(Keys.tenantDiff(TenantId.of("acme"))).isPresent(),
                "and the tenant's diff should exist even when the news is bad");
        }
    }

    @Test
    void twoProcessesSharingAnIdentityAreWarnedAboutRatherThanLeftToCollide() throws Exception {
        // Not detectable with certainty from one observation -- a record left by an unclean shutdown looks the
        // same as a live peer -- so the warning names both possibilities. Silence here would let somebody who
        // copied a deployment template run two supervisors that both write to one pool.
        Main.run(new String[] {"--store=" + storeUri(), "--id=fixed", "--max-passes=1", "--interval=1ms"});
        captured.reset();

        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            store.put(Keys.member("fixed"), "{}".getBytes(StandardCharsets.UTF_8));
        }
        Main.run(new String[] {"--store=" + storeUri(), "--id=fixed", "--max-passes=1", "--interval=1ms"});

        assertTrue(output().contains("a member record already exists for this id"), output());
        assertTrue(output().contains("Check --id"), output());
    }

    @Test
    void aCleanShutdownLeavesNoMemberRecordBehind() throws Exception {
        // So that a rolling restart is invisible rather than a series of gaps: the next process claims the pool
        // on its next pass instead of waiting out a lease.
        Main.run(new String[] {
            "--store=" + storeUri(), "--id=tidy", "--write", "--max-passes=2", "--interval=1ms"
        });

        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            assertTrue(store.get(Keys.member("tidy")).isEmpty(), "shutdown should have removed it");
        }
    }

    private void seedDesiredState() throws Exception {
        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            PoolId pool = PoolId.of("shared-a");
            TenantId tenant = TenantId.of("acme");
            store.put(Keys.poolDefinition(pool),
                new PoolDefinition(pool, PoolKind.SHARED, false, null, null, List.of()).toBytes());
            store.put(Keys.poolBinding(tenant), PoolBinding.to(pool).toBytes());
            store.put(Keys.tenantRegistration(tenant),
                "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));
            store.put(Keys.indicesPrefix(tenant) + "my-index", IndexConfig.toDocument(
                "my-index", "uzKxLWZu4nPSrTknEcDpoA", true,
                IndexConfig.settingsOf(2, 1, Replication.SEGMENT)
            ));
        }
    }

    // ---------------------------------------------------------------- --define-pool

    @Test
    void definePoolWritesADefinitionAndExits() throws Exception {
        // The gap this closes: until a pool exists there are no pools, so no reconciliation runs at all, and the
        // whole system looks broken while behaving exactly as designed.
        int code = Main.run(new String[] { "--store=" + storeUri(), "--define-pool=shared-a" });

        assertEquals(0, code);
        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            PoolDefinition written = PoolDefinition.from(store.get(Keys.poolDefinition(PoolId.of("shared-a"))).orElseThrow().value());
            assertEquals(PoolId.of("shared-a"), written.pool());
            assertEquals(PoolKind.SHARED, written.kind());
            // Default by default: a pool that serves nothing is the state the operator is trying to leave.
            assertTrue(written.isDefault());
            assertTrue(written.awarenessAttributes().isEmpty());
        }
        assertTrue(output().contains("defined pool"), output());
    }

    @Test
    void definePoolDoesNotStartASupervisor() throws Exception {
        // A one-shot action: no lease is taken and no pass runs, so it is safe to point at a store some other
        // supervisor already owns.
        Main.run(new String[] { "--store=" + storeUri(), "--define-pool=shared-a" });

        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            assertTrue(store.scanPrefix("supervisor/pools/").isEmpty(), "no lease may be taken");
            assertTrue(store.scanPrefix("supervisor/status/").isEmpty(), "no pass may run");
        }
        assertFalse(output().contains("running"), output());
    }

    @Test
    void definePoolRefusesToOverwriteAnExistingDefinition() throws Exception {
        // A definition carries an operator's tuning, and the command line it would be overwritten from was
        // probably copied out of a README.
        Main.run(new String[] { "--store=" + storeUri(), "--define-pool=shared-a", "--pool-awareness=zone" });
        int second = Main.run(new String[] { "--store=" + storeUri(), "--define-pool=shared-a" });

        assertEquals(1, second);
        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            PoolDefinition kept = PoolDefinition.from(store.get(Keys.poolDefinition(PoolId.of("shared-a"))).orElseThrow().value());
            assertEquals(List.of("zone"), kept.awarenessAttributes());
        }
        assertTrue(output().contains("already defined"), output());
    }

    @Test
    void definePoolCarriesTheOptionsItWasGiven() throws Exception {
        int code = Main.run(
            new String[] {
                "--store=" + storeUri(),
                "--define-pool=dedicated-acme",
                "--pool-kind=dedicated",
                "--pool-default=false",
                "--pool-awareness=zone,rack",
                "--pool-max-copies-per-node=500" }
        );

        assertEquals(0, code);
        try (KeyValueStore store = KeyValueStores.open(storeUri())) {
            PoolDefinition written = PoolDefinition.from(
                store.get(Keys.poolDefinition(PoolId.of("dedicated-acme"))).orElseThrow().value()
            );
            assertEquals(PoolKind.DEDICATED, written.kind());
            assertFalse(written.isDefault());
            assertEquals(List.of("zone", "rack"), written.awarenessAttributes());
            assertEquals(Integer.valueOf(500), written.maxShardCopiesPerNode());
        }
    }

    @Test
    void aPoolOptionWithoutDefinePoolIsRefusedRatherThanIgnored() {
        // The same reasoning as rejecting an unknown option: a stray --pool-kind in a deployment template is a
        // mistake somebody wants to hear about, not one to silently drop.
        int code = Main.run(new String[] { "--store=" + storeUri(), "--pool-kind=dedicated" });

        assertEquals(2, code);
        assertTrue(output().contains("only means anything with --define-pool"), output());
    }

    @Test
    void anInvalidPoolIdIsRefusedWithTheRule() {
        int code = Main.run(new String[] { "--store=" + storeUri(), "--define-pool=not/a/pool" });

        assertEquals(2, code);
        assertTrue(output().contains("--define-pool"), output());
    }
}
