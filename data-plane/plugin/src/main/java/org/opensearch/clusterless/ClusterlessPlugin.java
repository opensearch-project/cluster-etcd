/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.Registration;
import org.opensearch.clusterless.apply.AssignmentPoller;
import org.opensearch.clusterless.apply.ClusterApplierServiceStateApplier;
import org.opensearch.clusterless.apply.DataNodeState;
import org.opensearch.clusterless.apply.DesiredStateResolver;
import org.opensearch.clusterless.apply.MapperServiceMappingSource;
import org.opensearch.clusterless.apply.NodeEnvironmentShardStore;
import org.opensearch.clusterless.apply.StoreIndexConfigSource;
import org.opensearch.clusterless.publish.ClusterStateInventorySource;
import org.opensearch.clusterless.publish.IndicesServiceShardSizes;
import org.opensearch.clusterless.publish.LocalCapacityProbe;
import org.opensearch.clusterless.publish.NodePublisher;
import org.opensearch.clusterless.rest.RestClusterlessStatusAction;
import org.opensearch.clusterless.store.StoreConnection;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * The third plane: a node acting on desired state read from a shared key/value store, with no cluster
 * manager anywhere in the deployment.
 *
 * <p>The joke that explains the design better than a paragraph would: this plugin is the server's voice in
 * its head, pretending to be the cluster manager. OpenSearch already has a well-worn path for "the cluster
 * manager published new state, apply it" — the plugin's job is to walk that path with state it read from a
 * store rather than state it received from an elected node.
 *
 * <p><b>At this milestone it publishes and does not apply.</b> The three documents this node owns are
 * written; nothing is read and no cluster state is fabricated. That order is deliberate: it is the half of
 * the contract everything else depends on, a real supervisor can already see a live, placeable node, and
 * nothing can go wrong yet because nothing has been applied.
 *
 * <h2>The extension points, and why these</h2>
 *
 * <ul>
 *   <li>{@link #isClusterless()} — an actual upstream extension point for exactly this. The node then
 *       starts with {@code LocalClusterService} and {@code LocalDiscovery} and never looks for a manager.
 *   <li>{@link #getExecutorBuilders} — a dedicated pool for publishing, so a slow store cannot consume
 *       threads that queries need.
 *   <li>{@link #getGuiceServiceClasses} — the ugly, load-bearing one. See {@link GuiceHolder}.
 *   <li>{@link #onNodeStarted} rather than {@code createComponents}, because publishing needs the local
 *       node's identity and address, and those do not exist yet when components are created.
 * </ul>
 */
public class ClusterlessPlugin extends Plugin implements ClusterPlugin, ActionPlugin {

    private static final Logger logger = LogManager.getLogger(ClusterlessPlugin.class);

    private static final AtomicReference<GuiceHolder> GUICE_HOLDER = new AtomicReference<>();

    private Settings settings;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private NodeEnvironment nodeEnvironment;

    private volatile KeyValueStore store;
    private volatile NodePublisher publisher;
    private volatile AssignmentPoller poller;
    private volatile ScheduledPasses passes;

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.settings = environment.settings();
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.nodeEnvironment = nodeEnvironment;

        // Opened here rather than in onNodeStarted so that a bad store URI fails the node while it is
        // still starting, with the settings that caused it in scope, instead of after it is accepting
        // traffic it cannot serve.
        this.store = StoreConnection.open(ClusterlessSettings.storeUri(settings));
        return Collections.emptyList();
    }

    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        NodeId nodeId = NodeId.of(localNode.getId());
        String ephemeralId = localNode.getEphemeralId();

        ClusterStateInventorySource inventorySource = new ClusterStateInventorySource(
            clusterService::state,
            localNode.getId(),
            new IndicesServiceShardSizes(this::indicesService)
        );

        publisher = new NodePublisher(
            store,
            nodeId,
            ephemeralId,
            () -> registrationOf(localNode),
            inventorySource,
            new LocalCapacityProbe(nodeEnvironment),
            System::currentTimeMillis
        );

        boolean apply = ClusterlessSettings.APPLY.get(settings);
        poller = new AssignmentPoller(
            store,
            nodeId,
            new DesiredStateResolver(new StoreIndexConfigSource(store)),
            clusterService::state,
            desired -> new DataNodeState(
                localNode,
                desired,
                new MapperServiceMappingSource(this::indicesService),
                new NodeEnvironmentShardStore(nodeEnvironment)
            ),
            publisher,
            new ClusterApplierServiceStateApplier(clusterService.getClusterApplierService()),
            apply
        );

        if (apply == false) {
            logger.info(
                "clusterless: read-only. This node publishes what it holds and reads what it is assigned, and "
                    + "logs the cluster state it would build without applying it. Set [{}] to true to act on it.",
                ClusterlessSettings.APPLY.getKey()
            );
        }

        // The publishing pass first, so that a node is visible to the supervisor before it starts reading
        // instructions from it -- an assignment can only exist for a node the supervisor has already seen.
        passes = new ScheduledPasses(threadPool);
        passes.start("publishing", publisher::publish, ClusterlessSettings.HEARTBEAT_INTERVAL.get(settings));
        passes.start("polling for an assignment", poller::poll, ClusterlessSettings.POLL_INTERVAL.get(settings));
    }

    /** {@code null} until Guice has built the holder, which every caller has to treat as ordinary. */
    private IndicesService indicesService() {
        GuiceHolder holder = GUICE_HOLDER.get();
        return holder == null ? null : holder.indicesService;
    }

    /**
     * What this node says about itself.
     *
     * <p>Rebuilt on every pass rather than captured once, so that the "and when an attribute changes" half
     * of D3.1 is a property of the code rather than a promise. In practice nothing here changes while a
     * node is running, and the publisher compares before writing, so the document is written once.
     *
     * <p>The address is the <b>transport</b> address, matching what the contract's example shows and what
     * {@code tenant/<t>/status/nodes/<id>} projects. That is the right answer for a coordinator resolving
     * a data node, which talks to it over the transport protocol — but it is worth knowing that the
     * contract has room for exactly one address, so a reader wanting the HTTP endpoint cannot get it from
     * here.
     */
    private Registration registrationOf(DiscoveryNode localNode) {
        return new Registration(
            NodeId.of(localNode.getId()),
            localNode.getEphemeralId(),
            localNode.getName(),
            ClusterlessSettings.pool(settings),
            ClusterlessSettings.rolesFor(settings, localNode.isDataNode(), localNode.getRoles().isEmpty() == false),
            localNode.getAddress().getAddress(),
            localNode.getAddress().getPort(),
            Version.CURRENT.toString(),
            localNode.getAttributes(),
            ClusterlessSettings.HEARTBEAT_INTERVAL.get(settings).millis(),
            System.currentTimeMillis()
        );
    }

    /**
     * Node settings and index settings together, because the server collects both from here.
     *
     * <p>The index settings are not optional and not decoration. {@code IndicesService.createIndexService}
     * validates every index setting against the registry this list feeds, and an unregistered key throws
     * rather than being ignored — so leaving them out would mean every index this plugin fabricates failed
     * to be created. See {@link ClusterlessIndexSettings}.
     */
    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(ClusterlessSettings.all());
        settings.addAll(ClusterlessIndexSettings.all());
        return List.copyOf(settings);
    }

    /**
     * True, and this is the whole premise: the node then starts with no discovery and no election, and
     * nothing anywhere waits to meet a cluster manager.
     */
    @Override
    public boolean isClusterless() {
        return true;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(ScheduledPasses.executorBuilder(settings));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestClusterlessStatusAction(() -> publisher, () -> poller));
    }

    /**
     * Captures {@link IndicesService}, which {@code createComponents} cannot reach.
     *
     * <p>Ugly, and load-bearing. {@code cluster-etcd} needed this hack and the question of whether current
     * OpenSearch offers a cleaner path was left open; the answer, checked against 3.9, is <b>no</b>.
     * Neither {@code createComponents} overload receives {@code IndicesService} and it is not in the
     * plugin component registry either, so a Guice-injected lifecycle component whose only job is to hold
     * a reference remains the only way in. Two things need it: shard sizes for the inventory, and mapping
     * canonicalisation when cluster states start being fabricated.
     *
     * <p>The static field is what makes it work and what makes it unpleasant. Guice constructs this, not
     * the plugin, so there is no instance handle to pass — and a second node in the same JVM, which the
     * server's own integration tests routinely start, would share it. Anything reading it must therefore
     * treat a null as ordinary rather than assuming it is set.
     */
    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return List.of(GuiceHolder.class);
    }

    public static class GuiceHolder extends AbstractLifecycleComponent {

        private final IndicesService indicesService;

        @Inject
        public GuiceHolder(IndicesService indicesService) {
            this.indicesService = indicesService;
        }

        @Override
        protected void doStart() {
            GUICE_HOLDER.set(this);
        }

        @Override
        protected void doStop() {
            GUICE_HOLDER.compareAndSet(this, null);
        }

        @Override
        protected void doClose() throws IOException {}
    }

    /**
     * Shut down in order: stop publishing, then close the store.
     *
     * <p>The other way round would have a pass in flight write to a closed client, which surfaces as an
     * exception during shutdown that looks like a real failure and is not.
     */
    @Override
    public void close() {
        if (passes != null) {
            passes.close();
        }
        if (store != null) {
            store.close();
        }
        logger.info("clusterless: stopped");
    }
}
