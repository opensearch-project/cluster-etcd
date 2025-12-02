/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The {@link org.testcontainers.images.TimeLimitedLoggedPullImageResultCallback} instance used by test containers,
 *  creates a watcher daemon thread which is never
 * stopped. This filter excludes that thread from the thread leak detection logic. It also excludes ryuk resource reaper
 * thread aws IdleConnectionReaper thread, which are not closed on time .
 */
public final class TestContainerThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("testcontainers-pull-watchdog-")
            || t.getName().startsWith("testcontainers-ryuk")
            || t.getName().startsWith("ducttape-");
    }
}
