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
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.etcd.changeapplier.NodeStateApplier;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class ETCDWatcher implements Closeable {
    public static final String THREAD_POOL_NAME = "etcd-watcher";
    private final Logger logger = LogManager.getLogger(ETCDWatcher.class);
    private final ETCDClientHolder etcdClientHolder;
    private final DiscoveryNode localNode;
    private final NodeStateApplier nodeStateApplier;
    private final AtomicReference<Runnable> pendingAction = new AtomicReference<>();
    private final ThreadPool threadPool;
    private final String clusterName;
    private final Map<String, Watch.Watcher> additionalWatchers = new HashMap<>();
    private final NodeListener nodeListener = new NodeListener();
    private final ByteSequence nodeGoalStateKey;

    // The following is non-final, because it may be recreated in restartWatcher.
    private Watch.Watcher nodeWatcher;

    public ETCDWatcher(
        DiscoveryNode localNode,
        ByteSequence nodeGoalStateKey,
        NodeStateApplier nodeStateApplier,
        ETCDClientHolder etcdClientHolder,
        ThreadPool threadPool,
        String clusterName
    ) throws IOException, ExecutionException, InterruptedException {
        this.localNode = localNode;
        this.nodeGoalStateKey = nodeGoalStateKey;
        this.etcdClientHolder = etcdClientHolder;
        this.nodeStateApplier = nodeStateApplier;
        this.threadPool = threadPool;
        this.clusterName = clusterName;
        loadState(true);
        nodeWatcher = etcdClientHolder.getClient()
            .getWatchClient()
            .watch(nodeGoalStateKey, WatchOption.builder().withRevision(0).build(), nodeListener);
    }

    @Override
    public void close() {
        nodeWatcher.close();
        for (Watch.Watcher watcher : additionalWatchers.values()) {
            watcher.close();
        }
    }

    public static ExecutorBuilder<?> createExecutorBuilder(Settings settings) {
        return new FixedExecutorBuilder(settings, THREAD_POOL_NAME, 1, 100, THREAD_POOL_NAME);
    }

    private void loadState(boolean initialLoad) throws ExecutionException, InterruptedException {
        // Load the goal state of the node from etcd
        try (KV kvClient = etcdClientHolder.getClient().getKVClient()) {
            List<KeyValue> kvs = kvClient.get(nodeGoalStateKey).get().getKvs();
            if (kvs != null && kvs.isEmpty() == false && kvs.getFirst() != null) {
                handleNodeChange(kvs.getFirst(), initialLoad);
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
                        scheduleRefresh(() -> handleNodeChange(event.getKeyValue(), false));
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
            threadPool.schedule(() -> {
                Runnable action = pendingAction.get();
                if (action != null) {
                    try {
                        action.run();
                    } catch (Exception e) {
                        logger.error("Error while processing pending action", e);
                    }
                }
                pendingAction.compareAndSet(action, null);
            }, TimeValue.timeValueMillis(100), THREAD_POOL_NAME);
        }

        private static final Set<ErrorCode> FATAL_ERRORS = EnumSet.of(ErrorCode.INTERNAL, ErrorCode.UNKNOWN);

        @Override
        public void onError(Throwable throwable) {
            if (throwable instanceof EtcdException e) {
                logger.error("Error in node watcher with error code ({})", e.getErrorCode(), e);
                if (FATAL_ERRORS.contains(e.getErrorCode())) {
                    scheduleRefresh(ETCDWatcher.this::restartWatchers);
                }
            } else {
                logger.error("Error in node watcher", throwable);
            }
        }

        @Override
        public void onCompleted() {

        }
    }

    private synchronized void restartWatchers() {
        try {
            nodeWatcher.close();
        } catch (Exception e) {
            logger.error("Error while closing node watcher", e);
        }
        for (Watch.Watcher watcher : additionalWatchers.values()) {
            try {
                watcher.close();
            } catch (Exception e) {
                logger.error("Error while closing additional watcher", e);
            }
        }
        etcdClientHolder.resetClient();
        Client client = etcdClientHolder.getClient();
        nodeWatcher = client.getWatchClient().watch(nodeGoalStateKey, WatchOption.builder().withRevision(0).build(), nodeListener);
        Set<String> keysToWatch = new HashSet<>(additionalWatchers.keySet());
        for (String keyToWatch : keysToWatch) {
            ByteSequence watchKey = ByteSequence.from(keyToWatch, java.nio.charset.StandardCharsets.UTF_8);
            Watch.Watcher watcher = client.getWatchClient().watch(watchKey, WatchOption.builder().withRevision(0).build(), nodeListener);
            additionalWatchers.put(keyToWatch, watcher);
        }
        // Trigger a state reload in case we missed any changes while the watchers wer down
        try {
            loadState(false);
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Error while reloading node state", e);
        }
    }

    private void handleNodeChange(KeyValue keyValue, boolean isInitialLoad) {
        try {
            ByteSequence goalState;
            Client client = etcdClientHolder.getClient();
            try (KV kvClient = client.getKVClient()) {
                List<KeyValue> kvs = kvClient.get(nodeGoalStateKey).get().getKvs();
                goalState = kvs.getFirst().getValue();
            }
            ETCDStateDeserializer.NodeStateResult nodeStateResult = ETCDStateDeserializer.deserializeNodeState(
                localNode,
                goalState,
                client,
                clusterName,
                isInitialLoad
            );
            for (String keyToWatch : nodeStateResult.keysToWatch()) {
                if (additionalWatchers.containsKey(keyToWatch) == false) {
                    ByteSequence watchKey = ByteSequence.from(keyToWatch, java.nio.charset.StandardCharsets.UTF_8);
                    Watch.Watcher watcher = client.getWatchClient()
                        .watch(watchKey, WatchOption.builder().withRevision(0).build(), nodeListener);
                    additionalWatchers.put(keyToWatch, watcher);
                }
            }
            List<String> keysToRemove = additionalWatchers.keySet()
                .stream()
                .filter(key -> nodeStateResult.keysToWatch().contains(key) == false)
                .toList();
            for (String keyToRemove : keysToRemove) {
                Watch.Watcher watcher = additionalWatchers.remove(keyToRemove);
                if (watcher != null) {
                    watcher.close();
                }
            }
            String action = isInitialLoad ? "initial-load" : "update-node";
            nodeStateApplier.applyNodeState(action + " on change to " + keyValue.getKey().toString(), nodeStateResult.nodeState());
        } catch (Exception e) {
            logger.error("Error while processing node state", e);
        }
    }

    private void removeNode(KeyValue keyValue) {
        nodeStateApplier.removeNode("remove-node " + keyValue.getKey().toString(), localNode);
    }
}
