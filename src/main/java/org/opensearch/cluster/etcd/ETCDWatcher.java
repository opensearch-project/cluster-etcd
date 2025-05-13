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
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.etcd.changeapplier.ChangeApplierService;
import org.opensearch.cluster.etcd.changeapplier.NodeState;
import org.opensearch.cluster.node.DiscoveryNode;

import java.io.Closeable;
import java.io.IOException;

public class ETCDWatcher implements Closeable{
    private final Logger logger = LogManager.getLogger(ETCDWatcher.class);
    private final Client etcdClient;
    private final DiscoveryNode localNode;
    private final Watch.Watcher nodeWatcher;
    private final ChangeApplierService changeApplierService;

    public ETCDWatcher(DiscoveryNode localNode, ByteSequence nodeKey, ChangeApplierService changeApplierService) {
        this.localNode = localNode;
        this.changeApplierService = changeApplierService;
        // Initialize the etcd client
        this.etcdClient = Client.builder().endpoints("http://localhost:2379").build();
        nodeWatcher = etcdClient.getWatchClient().watch(nodeKey, new NodeListener());
        etcdClient.getWatchClient().requestProgress();
    }


    @Override
    public void close() {
        nodeWatcher.close();
        etcdClient.close();
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
                        handleNodeChange(event.getKeyValue());
                        break;
                    case DELETE:
                        // Handle node removal
                        logger.debug("Node removed");
                        removeNode(event.getPrevKV());
                        break;
                    default:
                        break;
                }
            }

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
            NodeState nodeState = ETCDStateDeserializer.deserializeNodeState(localNode, keyValue.getValue(), etcdClient);
            changeApplierService.applyNodeState("update-node " + keyValue.getKey().toString(), nodeState);
        } catch (IOException e) {
            logger.error("Error while reading node state", e);
        }
    }

    private void removeNode(KeyValue keyValue) {
        changeApplierService.removeNode("remove-node " + keyValue.getKey().toString(), localNode);
    }
}
