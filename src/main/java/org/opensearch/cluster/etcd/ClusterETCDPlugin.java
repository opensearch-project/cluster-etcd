/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;

import org.opensearch.cluster.etcd.changeapplier.ChangeApplierService;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
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
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class ClusterETCDPlugin extends Plugin implements ClusterPlugin {
    private ClusterService clusterService;
    private ETCDWatcher etcdWatcher;
    private ETCDHeartbeat etcdHeartbeat;
    private Client etcdClient;
    private NodeEnvironment nodeEnvironment;

    @Override
    public Collection<Object> createComponents(org.opensearch.transport.client.Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry, Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.clusterService = clusterService;
        this.nodeEnvironment = nodeEnvironment;
        return Collections.emptySet();
    }

    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        
        try {
             // Initialize the etcd client. TODO: Read config from cluster settings
            etcdClient = Client.builder().endpoints("http://127.0.0.1:2379").build();
            
            String clusterName = clusterService.getClusterName().value();
            
            etcdWatcher = new ETCDWatcher(localNode, getNodeKey(localNode),
                new ChangeApplierService(clusterService.getClusterApplierService()), etcdClient, clusterName);
            
            etcdHeartbeat = new ETCDHeartbeat(localNode, etcdClient, nodeEnvironment, clusterService);
            etcdHeartbeat.start();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ByteSequence getNodeKey(DiscoveryNode localNode) {
        return ByteSequence.from(localNode.getName(), StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
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
}
