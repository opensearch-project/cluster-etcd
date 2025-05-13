/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd;

import io.etcd.jetcd.ByteSequence;
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
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

public class ClusterETCDPlugin extends Plugin implements ClusterPlugin {
    private ClusterService clusterService;
    private ETCDWatcher etcdWatcher;

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry, Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.clusterService = clusterService;
        return Collections.emptySet();
    }

    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        etcdWatcher = new ETCDWatcher(localNode, ByteSequence.from(localNode.getId(), StandardCharsets.UTF_8),
                new ChangeApplierService(clusterService.getClusterApplierService()));
    }

    @Override
    public void close() throws IOException {
        etcdWatcher.close();
    }
}
