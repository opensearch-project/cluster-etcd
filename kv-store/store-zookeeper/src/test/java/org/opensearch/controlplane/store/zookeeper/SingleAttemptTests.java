/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The retry policy that turns Curator's retry off for one conditional write, checked without a server.
 *
 * <p>A retried conditional write meets its own first attempt and reports it as a conflict, so this has to
 * refuse inside {@code run} and nowhere else: reads still need the retry to wait out a reconnection.
 */
class SingleAttemptTests {

    private static final RetryPolicy ALWAYS = (count, elapsed, sleeper) -> true;
    private static final RetrySleeper NO_SLEEP = (time, unit) -> {};

    @Test
    void outsideASingleAttemptTheClientsPolicyDecides() {
        ZooKeeperKeyValueStore.SingleAttempt policy = new ZooKeeperKeyValueStore.SingleAttempt(ALWAYS);

        assertTrue(policy.allowRetry(0, 0, NO_SLEEP));
        assertTrue(policy.allowRetry(new KeeperException.ConnectionLossException()));
    }

    @Test
    void insideASingleAttemptNothingIsRetried() throws Exception {
        ZooKeeperKeyValueStore.SingleAttempt policy = new ZooKeeperKeyValueStore.SingleAttempt(ALWAYS);
        AtomicBoolean byCount = new AtomicBoolean(true);
        AtomicBoolean byException = new AtomicBoolean(true);

        ZooKeeperKeyValueStore.SingleAttempt.run(() -> {
            byCount.set(policy.allowRetry(0, 0, NO_SLEEP));
            byException.set(policy.allowRetry(new KeeperException.ConnectionLossException()));
            return new Stat();
        });

        assertFalse(byCount.get());
        assertFalse(byException.get());
        assertTrue(policy.allowRetry(0, 0, NO_SLEEP), "and the next operation is retried again");
    }

    @Test
    void aFailedAttemptStillRestoresTheRetry() {
        ZooKeeperKeyValueStore.SingleAttempt policy = new ZooKeeperKeyValueStore.SingleAttempt(ALWAYS);

        org.junit.jupiter.api.Assertions.assertThrows(KeeperException.ConnectionLossException.class,
            () -> ZooKeeperKeyValueStore.SingleAttempt.run(() -> {
                throw new KeeperException.ConnectionLossException();
            }));
        assertTrue(policy.allowRetry(0, 0, NO_SLEEP));
    }
}
