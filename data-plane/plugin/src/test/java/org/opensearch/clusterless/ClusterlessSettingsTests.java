/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless;

import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.PoolId;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;
import java.util.Set;


public class ClusterlessSettingsTests extends OpenSearchTestCase {

    public void testANodeWithNoStoreIsRejectedWithAReasonRatherThanADefault() {
        // A default would let a misconfigured node start happily against a private store where it is the
        // only participant, which looks exactly like a healthy node that no supervisor can see.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ClusterlessSettings.storeUri(Settings.EMPTY)
        );
        assertTrue(e.getMessage().contains("clusterless.store"));
        assertTrue(e.getMessage().contains("no cluster manager"));
    }

    public void testANodeMayDeclareNoPool() {
        // Legal and visible, not an error: pools/nodes/<id> may still answer, and if neither does the node
        // is NODE_UNPOOLED -- one node stranded visibly, rather than a pool invented from a typo.
        assertNull(ClusterlessSettings.pool(Settings.EMPTY));
        assertEquals(PoolId.of("shared-a"), ClusterlessSettings.pool(Settings.builder().put("clusterless.node.pool", "shared-a").build()));
    }

    public void testRolesAreDerivedFromWhatOpenSearchThinksTheNodeIs() {
        // The two vocabularies do not line up: OpenSearch has no coordinator role, and a coordinating-only
        // node is one with no roles at all.
        assertEquals(EnumSet.of(NodeRole.DATA), ClusterlessSettings.rolesFor(Settings.EMPTY, true, true));
        assertEquals(EnumSet.of(NodeRole.COORDINATOR), ClusterlessSettings.rolesFor(Settings.EMPTY, false, false));
    }

    public void testADataNodeIsNotSilentlyAlsoACoordinator() {
        // Every OpenSearch node can coordinate a search, so a derivation based on capability would return
        // both roles for the whole fleet and let the supervisor place a tenant's routing anywhere. Which
        // nodes take coordinator duty is an operator's decision.
        Set<NodeRole> derived = ClusterlessSettings.rolesFor(Settings.EMPTY, true, true);
        assertEquals(Set.of(NodeRole.DATA), derived);
    }

    public void testConfiguredRolesOverrideTheDerivation() {
        Settings settings = Settings.builder().putList("clusterless.node.roles", "data", "coordinator").build();
        assertEquals(EnumSet.of(NodeRole.DATA, NodeRole.COORDINATOR), ClusterlessSettings.rolesFor(settings, false, false));
    }

    public void testAnUnknownRoleIsRejectedWithTheOnesThatExist() {
        Settings settings = Settings.builder().putList("clusterless.node.roles", "ingest").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ClusterlessSettings.rolesFor(settings, true, true));
        assertTrue(e.getMessage().contains("data and coordinator"));
    }

    public void testThePollIntervalDefaultsToTheHeartbeatIntervalAndIsSeparatelySettable() {
        // Two numbers with different costs -- one is a write by every node in the fleet, the other a read
        // of one small key -- so they are independent settings that happen to share a default.
        Settings shared = Settings.builder().put("clusterless.heartbeat.interval", "2s").build();
        assertEquals(TimeValue.timeValueSeconds(2), ClusterlessSettings.HEARTBEAT_INTERVAL.get(shared));
        assertEquals(TimeValue.timeValueSeconds(2), ClusterlessSettings.POLL_INTERVAL.get(shared));

        Settings split = Settings.builder().put("clusterless.heartbeat.interval", "5s").put("clusterless.poll.interval", "500ms").build();
        assertEquals(TimeValue.timeValueSeconds(5), ClusterlessSettings.HEARTBEAT_INTERVAL.get(split));
        assertEquals(TimeValue.timeValueMillis(500), ClusterlessSettings.POLL_INTERVAL.get(split));
    }

    public void testAnAbsurdlyShortIntervalIsRejected() {
        // Not a taste judgement. A staleness threshold derived from it -- six times the interval, by
        // default -- would drop below the length of an ordinary garbage collection pause, and a healthy
        // node would be declared lost and have its shards rebuilt elsewhere.
        Settings settings = Settings.builder().put("clusterless.heartbeat.interval", "1ms").build();
        expectThrows(IllegalArgumentException.class, () -> ClusterlessSettings.HEARTBEAT_INTERVAL.get(settings));

        Settings poll = Settings.builder().put("clusterless.poll.interval", "1ms").build();
        expectThrows(IllegalArgumentException.class, () -> ClusterlessSettings.POLL_INTERVAL.get(poll));
    }

    public void testEverySettingIsRegisteredSoTheNodeDoesNotRejectItAsUnknown() {
        // An OpenSearch node refuses to start on an unrecognised setting, so one missing from this list is
        // a node that will not boot with the configuration its own documentation describes.
        Set<String> registered = new java.util.HashSet<>();
        ClusterlessSettings.all().forEach(setting -> registered.add(setting.getKey()));
        assertEquals(
            Set.of(
                "clusterless.store",
                "clusterless.node.pool",
                "clusterless.node.roles",
                "clusterless.heartbeat.interval",
                "clusterless.poll.interval"
            ),
            registered
        );
    }
}
