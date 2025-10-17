/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;

import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.etcd.changeapplier.ChangeApplierService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ClusterETCDPlugin extends Plugin implements ClusterPlugin, ActionPlugin {
    private static final AtomicReference<GuiceHolder> GUICE_HOLDER_REF = new AtomicReference<>();
    private ClusterService clusterService;
    private ETCDWatcher etcdWatcher;
    private Client etcdClient;
    private NodeEnvironment nodeEnvironment;
    private ThreadPool threadPool;

    @Override
    public Collection<Object> createComponents(
        org.opensearch.transport.client.Client client,
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
        this.clusterService = clusterService;
        this.nodeEnvironment = nodeEnvironment;
        this.threadPool = threadPool;
        return Collections.emptySet();
    }

    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        try {
            // Initialize the etcd client. Supports comma-separated endpoints.
            String endpointSetting = clusterService.getClusterSettings().get(ETCD_ENDPOINT_SETTING);
            if (Strings.isNullOrEmpty(endpointSetting)) {
                throw new IllegalStateException(ETCD_ENDPOINT_SETTING.getKey() + " has not been set");
            }
            String[] endpoints = endpointSetting.split(",");

            etcdClient = Client.builder().endpoints(endpoints).build();

            String clusterName = clusterService.getClusterName().value();

            etcdWatcher = new ETCDWatcher(
                localNode,
                getNodeGoalStateKey(localNode, clusterName),
                new ChangeApplierService(clusterService.getClusterApplierService(), GUICE_HOLDER_REF.get().indicesService),
                etcdClient,
                threadPool,
                clusterName
            );

            new ETCDHeartbeat(localNode, etcdClient, nodeEnvironment, clusterService, threadPool).start();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ByteSequence getNodeGoalStateKey(DiscoveryNode localNode, String clusterName) {
        String goalStatePath = ETCDPathUtils.buildSearchUnitGoalStatePath(localNode, clusterName);
        return ByteSequence.from(goalStatePath, StandardCharsets.UTF_8);
    }

    public static final Setting<String> ETCD_ENDPOINT_SETTING = Setting.simpleString("cluster.etcd.endpoint", Setting.Property.NodeScope);

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(ETCD_ENDPOINT_SETTING);
    }

    @Override
    public boolean isClusterless() {
        return true;
    }

    @Override
    public void close() {
        if (etcdWatcher != null) {
            etcdWatcher.close();
        }
        if (etcdClient != null) {
            etcdClient.close();
        }
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(ETCDWatcher.createExecutorBuilder(settings), ETCDHeartbeat.createExecutorBuilder(settings));
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(new ClusterManagerActionFilter(clusterService));
    }

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
            GUICE_HOLDER_REF.set(this);
        }

        @Override
        protected void doStop() {
            GUICE_HOLDER_REF.set(null);
        }

        @Override
        protected void doClose() throws IOException {

        }
    }
}
