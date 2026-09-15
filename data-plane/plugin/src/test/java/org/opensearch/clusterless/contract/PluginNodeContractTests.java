/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.clusterless.publish.CapacityProbe;
import org.opensearch.clusterless.publish.NodePublisher;
import org.opensearch.supervisor.nodecontract.ContractCopy;
import org.opensearch.supervisor.nodecontract.ContractNode;
import org.opensearch.supervisor.nodecontract.NodeContractChecks;
import org.opensearch.supervisor.nodecontract.RecordingStore;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This plugin against the shared contract checks.
 *
 * <p>The checks live in the supervisor repository, which owns {@code NODE_CONTRACT.md}, and are the same ones
 * {@code fake-node} runs. That is the whole point: the two implementations share no production code — this
 * one writes JSON with XContent and the other with Jackson databind — so a suite each ran against its own
 * reader would happily pass on two documents neither could read from the other. Every check over there parses
 * these bytes with the <em>supervisor's</em> reader.
 *
 * <p><b>One test method for thirteen checks</b>, via {@link NodeContractChecks#runAll}, and that is a
 * deliberate trade. The checks cannot be inherited as {@code @Test} methods here: they are written for JUnit
 * 5 and this is an OpenSearch plugin, whose build runs JUnit 4 under {@code randomizedtesting}. Annotations
 * the runner does not recognise are not an error — they are zero tests discovered and a green build, which is
 * exactly how this suite would have stopped protecting anything. So the checks are data, this runs all of
 * them, and a check added over there runs here without this file being touched.
 *
 * <p>{@code runAll} reports every failure rather than the first, which matters because the checks overlap: one
 * broken field usually breaks several, and the set that failed identifies it faster than the first one alone.
 *
 * <p>Note what the adapter does not do: it does not drive an assignment, because at this milestone the plugin
 * does not read one. What it holds is supplied directly, which is the seam {@code ShardInventorySource} exists
 * for — a different route to the same states {@code fake-node} reaches by converging. The checks do not care
 * how, only what comes out.
 */
public class PluginNodeContractTests extends OpenSearchTestCase {

    public void testTheNodeSatisfiesTheSharedContractChecks() {
        NodeContractChecks.runAll(PluginNodeUnderTest::new);
    }

    private static final class PluginNodeUnderTest implements ContractNode {

        private static final org.opensearch.supervisor.model.NodeId NODE = org.opensearch.supervisor.model.NodeId.of(
            "pv1sDMfZRxSTMiW5VrbN9w"
        );
        private static final String EPHEMERAL = "OYidGJ1ITdC2mMrZEW0BNQ";

        private final AtomicLong now = new AtomicLong(1788969977000L);
        private final AtomicReference<List<ShardInventory.HeldCopy>> held = new AtomicReference<>(List.of());
        private final NodePublisher publisher;

        PluginNodeUnderTest(RecordingStore store) {
            this.publisher = new NodePublisher(
                store,
                NodeId.of(NODE.value()),
                EPHEMERAL,
                () -> new Registration(
                    NodeId.of(NODE.value()),
                    EPHEMERAL,
                    "data-3",
                    PoolId.of("shared-a"),
                    EnumSet.of(NodeRole.DATA),
                    "10.0.3.14",
                    9300,
                    "3.9.0",
                    Map.of("zone", "z1"),
                    5000,
                    0L
                ),
                held::get,
                CapacityProbe.UNKNOWN,
                now::get
            );
        }

        @Override
        public org.opensearch.supervisor.model.NodeId nodeId() {
            return NODE;
        }

        @Override
        public String ephemeralId() {
            return EPHEMERAL;
        }

        @Override
        public void start() {
            publisher.publish();
        }

        @Override
        public void pass() {
            publisher.publish();
        }

        @Override
        public void advanceClock(Duration by) {
            now.addAndGet(by.toMillis());
        }

        @Override
        public void holdServing(List<ContractCopy> copies) {
            List<ShardInventory.HeldCopy> translated = new ArrayList<>();
            for (ContractCopy copy : copies) {
                translated.add(
                    ShardInventory.HeldCopy.started(
                        TenantId.of(copy.tenant().value()),
                        copy.indexUuid(),
                        copy.indexName(),
                        copy.configRevision(),
                        copy.shard(),
                        roleOf(copy.role()),
                        4_823_012L
                    )
                );
            }
            held.set(List.copyOf(translated));
            publisher.publish();
        }

        /**
         * What this plugin makes of an assignment the supervisor wrote.
         *
         * <p>The half of the contract the suite used to be silent about, and the silence cost a running
         * coordinator: a {@code roles} array replaced a {@code role} string and this reader knew only the older
         * spelling. Reported here in terms of what the node <em>concluded</em> rather than as a parsed object,
         * because that both implementations can call a parser was never the thing in doubt.
         */
        @Override
        public ContractNode.Understood readAssignment(byte[] document) {
            Assignment assignment = Assignment.read(document);

            Map<org.opensearch.supervisor.model.TenantId, java.util.Set<org.opensearch.supervisor.model.node.NodeRole>> roles =
                new java.util.LinkedHashMap<>();
            List<ContractCopy> copies = new ArrayList<>();

            assignment.tenants().forEach((tenant, forTenant) -> {
                java.util.Set<org.opensearch.supervisor.model.node.NodeRole> understood = new java.util.LinkedHashSet<>();
                forTenant.roles().forEach(role -> understood.add(nodeRoleOf(role)));
                roles.put(org.opensearch.supervisor.model.TenantId.of(tenant.value()), understood);

                forTenant.indices().forEach((uuid, index) -> index.shards().forEach((shard, role) -> copies.add(
                    new ContractCopy(
                        org.opensearch.supervisor.model.TenantId.of(tenant.value()),
                        uuid,
                        index.name(),
                        index.configRevision(),
                        shard,
                        supervisorRoleOf(role)
                    )
                )));
            });
            return new ContractNode.Understood(roles, copies);
        }

        /** As with {@link #roleOf}, a switch so that a role added on either side is a compile error. */
        private static org.opensearch.supervisor.model.node.NodeRole nodeRoleOf(NodeRole role) {
            return switch (role) {
                case DATA -> org.opensearch.supervisor.model.node.NodeRole.DATA;
                case COORDINATOR -> org.opensearch.supervisor.model.node.NodeRole.COORDINATOR;
                case UNKNOWN -> org.opensearch.supervisor.model.node.NodeRole.UNKNOWN;
            };
        }

        private static org.opensearch.supervisor.model.node.ShardRole supervisorRoleOf(ShardRole role) {
            return switch (role) {
                case PRIMARY -> org.opensearch.supervisor.model.node.ShardRole.PRIMARY;
                case REPLICA -> org.opensearch.supervisor.model.node.ShardRole.REPLICA;
                case SEARCH_REPLICA -> org.opensearch.supervisor.model.node.ShardRole.SEARCH_REPLICA;
                case UNKNOWN -> org.opensearch.supervisor.model.node.ShardRole.UNKNOWN;
            };
        }

        /**
         * The two enums are the same three names in two repositories, which is what the checks are for. A
         * switch rather than {@code valueOf}, so that a role added on one side is a compile error here rather
         * than a runtime surprise in a document.
         */
        private static ShardRole roleOf(org.opensearch.supervisor.model.node.ShardRole role) {
            return switch (role) {
                case PRIMARY -> ShardRole.PRIMARY;
                case REPLICA -> ShardRole.REPLICA;
                case SEARCH_REPLICA -> ShardRole.SEARCH_REPLICA;
                case UNKNOWN -> throw new IllegalArgumentException("the checks must not ask a node to hold an unknown role");
            };
        }
    }
}
