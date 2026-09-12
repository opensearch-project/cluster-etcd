/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.clusterless.publish.CapacityProbe;
import org.opensearch.clusterless.publish.NodePublisher;
import org.opensearch.supervisor.nodecontract.ContractCopy;
import org.opensearch.supervisor.nodecontract.ContractNode;
import org.opensearch.supervisor.nodecontract.NodeContractTests;
import org.opensearch.supervisor.nodecontract.RecordingStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This plugin against the shared contract suite.
 *
 * <p>The suite lives in the supervisor repository, which owns {@code NODE_CONTRACT.md}, and is the same
 * one {@code fake-node} runs. That is the whole point: the two implementations share no production code —
 * this one writes JSON with XContent and the other with Jackson databind — so a suite each ran against its
 * own reader would happily pass on two documents neither could read from the other. Every assertion over
 * there parses these bytes with the <em>supervisor's</em> reader.
 *
 * <p>Note what the adapter does not do: it does not drive an assignment, because at this milestone the
 * plugin does not read one. What it holds is supplied directly, which is the seam
 * {@code ShardInventorySource} exists for — and is a different route to the same states {@code fake-node}
 * reaches by converging. The suite does not care how, only what comes out.
 */
class PluginNodeContractTests extends NodeContractTests {

    @Override
    protected ContractNode newNode(RecordingStore store) {
        return new PluginNodeUnderTest(store);
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
         * The two enums are the same three names in two repositories, which is what the suite is for. A
         * switch rather than {@code valueOf}, so that a role added on one side is a compile error here
         * rather than a runtime surprise in a document.
         */
        private static ShardRole roleOf(org.opensearch.supervisor.model.node.ShardRole role) {
            return switch (role) {
                case PRIMARY -> ShardRole.PRIMARY;
                case REPLICA -> ShardRole.REPLICA;
                case SEARCH_REPLICA -> ShardRole.SEARCH_REPLICA;
                case UNKNOWN -> throw new IllegalArgumentException("the suite must not ask a node to hold an unknown role");
            };
        }
    }
}
