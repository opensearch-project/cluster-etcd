/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStoreProvider;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;

/**
 * {@code zk://host1:2181,host2:2181/controlplane}
 *
 * <p>The authority is a ZooKeeper connection string and the path is the znode all keys live beneath. The
 * path is required: writing a control plane's keys at the ensemble root would collide with anything else
 * using it, and ZooKeeper reserves {@code /zookeeper}.
 */
public final class ZooKeeperKeyValueStoreProvider implements KeyValueStoreProvider {

    private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(60);

    @Override
    public String scheme() {
        return "zk";
    }

    @Override
    public KeyValueStore open(URI uri) throws IOException {
        String connectString = uri.getAuthority();
        if (connectString == null || connectString.isBlank()) {
            throw new IOException("store [" + uri + "] needs a ZooKeeper connection string: " + usage());
        }
        String root = uri.getPath();
        if (root == null || root.isBlank() || root.equals("/")) {
            throw new IOException(
                "store [" + uri + "] needs a root znode path, so that these keys cannot collide with "
                    + "anything else on the ensemble: " + usage());
        }
        if (root.endsWith("/")) {
            root = root.substring(0, root.length() - 1);
        }

        // defaultData is set explicitly because Curator's default is the local hostname's bytes, which
        // would give every parent container a payload that looks like data. The framing byte means such a
        // payload would not be read as a key anyway, but relying on that would be relying on an accident.
        RetryPolicy retry = new ExponentialBackoffRetry((int) Duration.ofMillis(200).toMillis(), 5);
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(connectString)
            .retryPolicy(retry)
            .connectionTimeoutMs((int) DEFAULT_CONNECT_TIMEOUT.toMillis())
            .sessionTimeoutMs((int) DEFAULT_SESSION_TIMEOUT.toMillis())
            .defaultData(new byte[0])
            .build();
        client.start();

        try {
            // Blocks until the ensemble answers, so a bad connection string fails at startup rather than
            // on the first request.
            if (client.blockUntilConnected(
                (int) DEFAULT_CONNECT_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS) == false) {
                throw new IOException("ZooKeeper at [" + connectString + "] did not answer within "
                    + DEFAULT_CONNECT_TIMEOUT.toSeconds() + "s");
            }
            if (client.checkExists().forPath(root) == null) {
                client.create().creatingParentContainersIfNeeded().forPath(root, new byte[0]);
            }
        } catch (IOException e) {
            client.close();
            throw e;
        } catch (Exception e) {
            client.close();
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IOException("could not prepare ZooKeeper root [" + root + "]: " + e, e);
        }
        return new ZooKeeperKeyValueStore(client, root, true);
    }

    @Override
    public String usage() {
        return "zk://host:2181[,host:2181]/root   ZooKeeper, one znode per key segment";
    }
}
