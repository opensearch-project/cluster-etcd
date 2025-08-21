/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.etcd.changeapplier.NodeState;
import org.opensearch.cluster.etcd.changeapplier.NodeStateApplier;
import org.opensearch.cluster.node.DiscoveryNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ETCDWatcher implements Closeable {
    private final Logger logger = LogManager.getLogger(ETCDWatcher.class);
    private final Client etcdClient;
    private final DiscoveryNode localNode;
    private final Watch.Watcher nodeWatcher;
    private final NodeStateApplier nodeStateApplier;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        r -> new Thread(r, "etcd-watcher-scheduler")
    );
    private final AtomicReference<Runnable> pendingAction = new AtomicReference<>();
    private final String clusterName;

    public ETCDWatcher(
        DiscoveryNode localNode,
        ByteSequence nodeGoalStateKey,
        NodeStateApplier nodeStateApplier,
        Client etcdClient,
        String clusterName
    ) throws IOException, ExecutionException, InterruptedException {
        this.localNode = localNode;
        this.etcdClient = etcdClient;
        this.nodeStateApplier = nodeStateApplier;
        this.clusterName = clusterName;
        loadState(nodeGoalStateKey);
        nodeWatcher = etcdClient.getWatchClient()
            .watch(nodeGoalStateKey, WatchOption.builder().withRevision(0).build(), new NodeListener());
    }

    @Override
    public void close() {
        nodeWatcher.close();
        scheduledExecutorService.close();
        etcdClient.close();
    }

    private void loadState(ByteSequence nodeKey) throws ExecutionException, InterruptedException {
        // Load the initial state of the node from etcd
        try (KV kvClient = etcdClient.getKVClient()) {
            List<KeyValue> kvs = kvClient.get(nodeKey).get().getKvs();
            if (kvs != null && kvs.isEmpty() == false && kvs.getFirst() != null) {
                handleNodeChange(kvs.getFirst());
            }
        }
    }

    private class NodeListener implements Watch.Listener {
        @Override
        public void onNext(WatchResponse watchResponse) {
            for (WatchEvent event : watchResponse.getEvents()) {
                switch (event.getEventType()) {
                    case PUT:
                        // Handle node addition/update
                        if (event.getPrevKV() != null) {
                            logger.debug("Node updated");
                        } else {
                            logger.debug("Node added");
                        }
                        scheduleRefresh(() -> handleNodeChange(event.getKeyValue()));
                        break;
                    case DELETE:
                        // Handle node removal
                        logger.debug("Node removed");
                        scheduleRefresh(() -> removeNode(event.getKeyValue()));
                        break;
                    default:
                        break;
                }
            }

        }

        private void scheduleRefresh(Runnable nextAction) {
            pendingAction.set(nextAction);
            scheduledExecutorService.schedule(() -> {
                Runnable action = pendingAction.get();
                if (action != null) {
                    try {
                        action.run();
                    } catch (Exception e) {
                        logger.error("Error while processing pending action", e);
                    }
                }
                pendingAction.compareAndSet(action, null);
            }, 100, TimeUnit.MILLISECONDS);
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("Error in node watcher", throwable);
        }

        @Override
        public void onCompleted() {

        }
    }

    private void handleNodeChange(KeyValue keyValue) {
        try {
            NodeState nodeState = ETCDStateDeserializer.deserializeNodeState(localNode, keyValue.getValue(), etcdClient, clusterName);
            nodeStateApplier.applyNodeState("update-node " + keyValue.getKey().toString(), nodeState);
            if (nodeState.hasConverged() == false) {
                // Schedule a reload of the node state if it has not converged
                scheduledExecutorService.schedule(() -> {
                    try {
                        logger.info("Reloading node state for key: {}", keyValue.getKey());
                        loadState(keyValue.getKey());
                    } catch (Exception e) {
                        logger.error("Error while reloading node state", e);
                    }
                }, 1, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            logger.error("Error while processing node state", e);
        }
    }

    private void removeNode(KeyValue keyValue) {
        nodeStateApplier.removeNode("remove-node " + keyValue.getKey().toString(), localNode);
    }
}
