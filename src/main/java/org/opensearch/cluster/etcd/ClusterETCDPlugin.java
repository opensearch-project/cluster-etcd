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
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class ClusterETCDPlugin extends Plugin implements ClusterPlugin, ActionPlugin {
    private ClusterService clusterService;
    private ETCDWatcher etcdWatcher;
    private ETCDHeartbeat etcdHeartbeat;
    private Client etcdClient;
    private NodeEnvironment nodeEnvironment;

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
                getNodeKey(localNode, clusterName),
                new ChangeApplierService(clusterService.getClusterApplierService()),
                etcdClient,
                clusterName
            );

            etcdHeartbeat = new ETCDHeartbeat(localNode, etcdClient, nodeEnvironment, clusterService);
            etcdHeartbeat.start();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ByteSequence getNodeKey(DiscoveryNode localNode, String clusterName) {
        String goalStatePath = ETCDPathUtils.buildSearchUnitGoalStatePath(clusterName, localNode.getName());
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
        if (etcdHeartbeat != null) {
            etcdHeartbeat.stop();
        }
        if (etcdWatcher != null) {
            etcdWatcher.close();
        }
        if (etcdClient != null) {
            etcdClient.close();
        }
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(new ClusterHealthActionFilter(clusterService));
    }
}
