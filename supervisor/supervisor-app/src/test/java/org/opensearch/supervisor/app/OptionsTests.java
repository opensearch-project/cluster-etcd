package org.opensearch.supervisor.app;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.TenantId;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OptionsTests {

    private static Options parse(String... args) {
        return Options.parse(args, Map.of());
    }

    @Test
    void readOnlyUnlessWriteIsGiven() {
        // The safety default, and the one worth a test of its own: this service deletes shard copies, so a
        // forgotten flag must cost an afternoon rather than a cluster's data.
        assertTrue(parse("--store=file:///tmp/x").readOnly());
        assertFalse(parse("--store=file:///tmp/x", "--write").readOnly());
    }

    @Test
    void theDefaultIdentityIsUniquePerProcessRatherThanStableAcrossRestarts() {
        // Uniqueness is a correctness property: two supervisors sharing an id would each believe they hold the
        // same pool and both write assignments for it. Stability would be tidier and is the wrong trade.
        String id = parse("--store=file:///tmp/x").supervisorId();

        assertTrue(id.endsWith("-" + ProcessHandle.current().pid()), id);
    }

    @Test
    void storeIsRequiredAndTheMessageSaysSo() {
        Options.UsageException thrown = assertThrows(Options.UsageException.class, () -> parse());

        assertTrue(thrown.getMessage().contains("--store is required"), thrown.getMessage());
        assertFalse(thrown.requested(), "nobody asked for the usage text");
    }

    @Test
    void helpIsNotAFailure() {
        // So a shell sees zero. Getting this wrong makes --help look like a broken container image.
        Options.UsageException thrown = assertThrows(Options.UsageException.class, () -> parse("--help"));

        assertTrue(thrown.requested());
        assertTrue(thrown.getMessage().contains("--store"), thrown.getMessage());
    }

    @Test
    void anUnknownOptionFailsRatherThanBeingIgnored() {
        // A silently ignored --dry-run is how somebody believes they are running read-only and is not.
        Options.UsageException thrown = assertThrows(Options.UsageException.class,
            () -> parse("--store=file:///tmp/x", "--dry-run"));

        assertTrue(thrown.getMessage().contains("unknown option [--dry-run]"), thrown.getMessage());
    }

    @Test
    void aDuplicatedOptionFailsRatherThanLastOneWinning() {
        // Last-wins hides a duplicated line in a deployment template, which is exactly where this happens.
        Options.UsageException thrown = assertThrows(Options.UsageException.class,
            () -> parse("--store=file:///a", "--store=file:///b"));

        assertTrue(thrown.getMessage().contains("more than once"), thrown.getMessage());
    }

    @Test
    void durationsAreWrittenTheWayPeopleWriteThem() {
        assertEquals(Duration.ofMillis(500), Options.duration("500ms", "x"));
        assertEquals(Duration.ofSeconds(5), Options.duration("5s", "x"));
        assertEquals(Duration.ofMinutes(2), Options.duration("2m", "x"));
        assertEquals(Duration.ofHours(1), Options.duration("1h", "x"));
    }

    @Test
    void aDurationWithoutAUnitIsRefusedRatherThanGuessed() {
        // "15" could be seconds or milliseconds, and the two differ by a factor of a thousand in how long a
        // pool stays unowned after a crash. Guessing is not available.
        Options.UsageException thrown = assertThrows(Options.UsageException.class,
            () -> Options.duration("15", "lease-ttl"));

        assertTrue(thrown.getMessage().contains("has no unit"), thrown.getMessage());
    }

    @Test
    void isoDurationsAreRejectedWithAHelpfulMessage() {
        assertThrows(Options.UsageException.class, () -> Options.duration("PT5S", "interval"));
    }

    @Test
    void theEnvironmentSuppliesDefaultsAndTheCommandLineWins() {
        Map<String, String> env = Map.of(
            "SUPERVISOR_STORE", "file:///from-env",
            "SUPERVISOR_LEASE_TTL", "30s",
            "SUPERVISOR_WRITE", "true"
        );

        Options fromEnv = Options.parse(new String[] {}, env);
        assertEquals("file:///from-env", fromEnv.storeUri());
        assertEquals(Duration.ofSeconds(30), fromEnv.leaseTtl());
        assertFalse(fromEnv.readOnly(), "the environment can enable writing too");

        Options overridden = Options.parse(new String[] {"--store=file:///from-args"}, env);
        assertEquals("file:///from-args", overridden.storeUri());
        assertEquals(Duration.ofSeconds(30), overridden.leaseTtl(), "unrelated env values still apply");
    }

    @Test
    void tenantsDefaultToBeingDiscovered() {
        assertEquals(java.util.Set.of(), parse("--store=file:///tmp/x").tenants());
        assertEquals(
            java.util.Set.of(TenantId.of("acme"), TenantId.of("globex")),
            parse("--store=file:///tmp/x", "--tenants=acme, globex").tenants()
        );
    }

    @Test
    void policyAndBalancerDefaultsMatchTheOnesTheCodeAlreadyUses() {
        // So that running the process is the same as running the tests. A default that drifts here would make
        // every test's conclusions inapplicable to the thing actually deployed.
        Options options = parse("--store=file:///tmp/x");

        assertEquals(org.opensearch.supervisor.model.plan.Planner.Policy.defaults(), options.policy());
        assertEquals(org.opensearch.supervisor.core.Balancer.Config.defaults(), options.balancer());
    }

    @Test
    void rebalancingCanBeTurnedOffFromTheCommandLine() {
        assertFalse(parse("--store=file:///tmp/x", "--rebalance=false").policy().rebalance());
    }

    @Test
    void aFlagThatWantsABooleanRefusesAnythingElse() {
        Options.UsageException thrown = assertThrows(Options.UsageException.class,
            () -> parse("--store=file:///tmp/x", "--rebalance=yes"));

        assertTrue(thrown.getMessage().contains("takes true or false"), thrown.getMessage());
    }

    @Test
    void whatTheProcessIsRunningWithEndsUpInTheStartupLine() {
        // Not cosmetic: the most common question about a running supervisor is what it thinks its configuration
        // is, and inferring it from a deployment template is how people get that wrong.
        java.util.List<String> described = parse("--store=file:///tmp/x", "--write").describe();

        assertTrue(described.contains("mode"));
        assertTrue(described.contains("write"));
        assertTrue(described.contains("store"));
    }
}
