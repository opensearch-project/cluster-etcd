package org.opensearch.supervisor.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * These assertions are the contract documents, executable. A change here should be accompanied by a change
 * to {@code KEY_LAYOUT.md}, {@code NODE_CONTRACT.md} or {@code SUPERVISOR_KEYS.md} — and if it is not, one
 * of the two is wrong.
 */
class KeysTests {

    private static final TenantId ACME = TenantId.of("acme");
    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final PoolId SHARED = PoolId.of("shared-a");

    @Test
    void controlPlaneKeysMatchTheDocumentedLayout() {
        assertEquals("tenant/acme/", Keys.tenantPrefix(ACME));
        assertEquals("tenant/acme/indices/", Keys.indicesPrefix(ACME));
        assertEquals("tenant/acme/aliases/", Keys.aliasesPrefix(ACME));
        assertEquals("tenant/acme/data_streams/", Keys.dataStreamsPrefix(ACME));
        assertEquals("tenant/acme/tasks/index-management/", Keys.tasksPrefix(ACME));
    }

    @Test
    void projectionsLiveInsideTheTenantPrefix() {
        // Not a stylistic choice: the control plane's handlers see a store rooted at tenant/<id>/, so a
        // projection outside it is unreachable to the service that has to read it.
        assertTrue(Keys.clusterStatus(ACME).startsWith(Keys.tenantPrefix(ACME)));
        assertTrue(Keys.indexStatus(ACME, "uzKx").startsWith(Keys.tenantPrefix(ACME)));
        assertTrue(Keys.nodeStatus(ACME, NODE).startsWith(Keys.tenantPrefix(ACME)));
        assertTrue(Keys.awarenessStatus(ACME).startsWith(Keys.tenantPrefix(ACME)));

        assertEquals("tenant/acme/status/cluster", Keys.clusterStatus(ACME));
        assertEquals("tenant/acme/status/indices/uzKx", Keys.indexStatus(ACME, "uzKx"));
        assertEquals("tenant/acme/status/awareness", Keys.awarenessStatus(ACME));
    }

    @Test
    void oneSegmentDecidesTheWriterUnderTheTenantPrefix() {
        // Everything the supervisor writes under a tenant is under status/; everything the control plane
        // writes is not. Ownership has to be decidable from the key alone.
        String status = Keys.statusPrefix(ACME);
        assertTrue(Keys.clusterStatus(ACME).startsWith(status));
        assertTrue(Keys.indexStatusPrefix(ACME).startsWith(status));
        assertTrue(Keys.nodeStatusPrefix(ACME).startsWith(status));

        assertTrue(Keys.indicesPrefix(ACME).startsWith(status) == false);
        assertTrue(Keys.tasksPrefix(ACME).startsWith(status) == false);
    }

    @Test
    void nodeKeysAreGroupedByKindNotByNode() {
        // The hot path is "read every heartbeat, read almost no inventories", and scanPrefix cannot skip a
        // middle segment. nodes/<id>/heartbeat would make one scan of nodes/ return every inventory.
        assertEquals("nodes/registration/pv1sDMfZRxSTMiW5VrbN9w", Keys.nodeRegistration(NODE));
        assertEquals("nodes/heartbeat/pv1sDMfZRxSTMiW5VrbN9w", Keys.nodeHeartbeat(NODE));
        assertEquals("nodes/shards/pv1sDMfZRxSTMiW5VrbN9w", Keys.nodeShards(NODE));
        assertEquals("nodes/assignment/pv1sDMfZRxSTMiW5VrbN9w", Keys.nodeAssignment(NODE));

        assertTrue(Keys.nodeHeartbeat(NODE).startsWith(Keys.NODE_HEARTBEAT_PREFIX));
        assertTrue(Keys.nodeShards(NODE).startsWith(Keys.NODE_HEARTBEAT_PREFIX) == false);
    }

    @Test
    void poolAndSupervisorKeys() {
        assertEquals("pools/definitions/shared-a", Keys.poolDefinition(SHARED));
        assertEquals("pools/bindings/acme", Keys.poolBinding(ACME));
        assertEquals("pools/nodes/pv1sDMfZRxSTMiW5VrbN9w", Keys.poolNode(NODE));

        assertEquals("supervisor/members/supervisor-a1b2", Keys.member("supervisor-a1b2"));
        assertEquals("supervisor/pools/shared-a/lease", Keys.poolLease(SHARED));
        assertEquals("supervisor/leases/housekeeping", Keys.HOUSEKEEPING_LEASE);
    }

    @Test
    void observabilityKeys() {
        assertEquals("supervisor/status/pools/shared-a/convergence", Keys.poolConvergence(SHARED));
        assertEquals("supervisor/status/pools/shared-a/oscillation", Keys.poolOscillation(SHARED));
        assertEquals("supervisor/status/members/supervisor-a1b2", Keys.memberStatus("supervisor-a1b2"));
        assertEquals("supervisor/status/housekeeping", Keys.HOUSEKEEPING_STATUS);

        // Keyed by tenant, not by pool: a rebind must not move an operator's bookmark.
        assertEquals("supervisor/status/diff/acme", Keys.tenantDiff(ACME));
    }

    @Test
    void tenantRegistryIsAtTheRoot() {
        // The whole point of the control plane change: discoverable with one small scan rather than by
        // scanning tenant/, which returns the entire store.
        assertEquals("tenants/acme", Keys.tenantRegistration(ACME));
        assertTrue(Keys.tenantRegistration(ACME).startsWith("tenant/") == false);
    }

    /**
     * Every prefix constant must end in {@code '/'}. {@code scanPrefix} is specified over key prefixes
     * rather than path boundaries, so {@code "tenant/acme/i"} is a legal prefix that matches
     * {@code indices/} and {@code ingest/} together — a prefix missing its slash is a scan that silently
     * returns too much.
     */
    @Test
    void everyPrefixConstantEndsInASlash() {
        List<String> offenders = new ArrayList<>();
        for (var field : Keys.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) == false) {
                continue;
            }
            if (field.getName().endsWith("_PREFIX") == false) {
                continue;
            }
            try {
                String value = (String) field.get(null);
                if (value.endsWith("/") == false) {
                    offenders.add(field.getName() + " = " + value);
                }
            } catch (IllegalAccessException e) {
                fail("could not read " + field.getName(), e);
            }
        }
        assertEquals(List.of(), offenders, "prefix constants must end in '/'");
    }

    /**
     * No key-building method may accept a raw {@code String} where an identifier type exists. The
     * identifier types are what enforce "no id contains a slash", and a {@code String} parameter is how
     * that protection gets bypassed by accident.
     *
     * <p>Two exemptions, both checked rather than assumed: {@code indexUuid} and {@code supervisorId} have
     * no type yet and are validated inline instead. Every builder exempted here is also in the escape test below.
     */
    @Test
    void keyBuildersTakeIdentifierTypes() {
        List<String> offenders = new ArrayList<>();
        for (Method method : Keys.class.getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers()) == false) {
                continue;
            }
            if (method.getName().equals("idAfter")) {
                continue;
            }
            for (var parameter : method.getParameters()) {
                if (parameter.getType() != String.class) {
                    continue;
                }
                if (method.getName().equals("indexStatus") || method.getName().equals("primaryTerms")
                    || method.getName().startsWith("member")) {
                    continue;
                }
                offenders.add(method.getName() + "(" + parameter.getType().getSimpleName() + ")");
            }
        }
        assertEquals(List.of(), offenders, "key builders should take identifier types, not raw strings");
    }

    @ParameterizedTest
    @ValueSource(strings = { "a/b", "../nodes", "" })
    void rawIdentifiersThatCouldEscapeTheirSubtreeAreRejected(String bad) {
        // A slash is the only character that can put a key somewhere it was not supposed to be, and an
        // index uuid arriving from a torn document must not be able to do that.
        assertThrows(IllegalArgumentException.class, () -> Keys.indexStatus(ACME, bad));
        assertThrows(IllegalArgumentException.class, () -> Keys.primaryTerms(ACME, bad));
        assertThrows(IllegalArgumentException.class, () -> Keys.member(bad));
    }

    @Test
    void dotDotIsAnOrdinaryKeySegmentAndIsNotRejected() {
        // Worth pinning rather than leaving to intuition. The store specifies '..' as an ordinary key
        // segment that never resolves — KeyValueStoreContractTests.dotDotIsAnOrdinaryKeySegment is part of
        // the contract every backend has to pass — so an id of ".." lands at a literal, harmless key rather
        // than traversing anywhere. Rejecting it here would be theatre, and would suggest the store needed
        // protecting from something it has already ruled out.
        assertEquals("tenant/acme/status/indices/..", Keys.indexStatus(ACME, ".."));
    }

    @Test
    void idAfterStripsThePrefix() {
        assertEquals("acme", Keys.idAfter(Keys.TENANTS_PREFIX, "tenants/acme"));
        assertEquals(
            "pv1sDMfZRxSTMiW5VrbN9w",
            Keys.idAfter(Keys.NODE_HEARTBEAT_PREFIX, "nodes/heartbeat/pv1sDMfZRxSTMiW5VrbN9w")
        );
        assertThrows(IllegalArgumentException.class, () -> Keys.idAfter(Keys.TENANTS_PREFIX, "tenant/acme/"));
    }
}
