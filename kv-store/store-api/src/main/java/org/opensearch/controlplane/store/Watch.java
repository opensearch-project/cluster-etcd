/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

/**
 * A registered interest in changes under a key prefix. Closing it stops notifications.
 *
 * <h2>A hint, not an event log</h2>
 *
 * <p>This deliberately carries no payload: no changed key, no old and new value, no revision, no ordering.
 * A notification means only <b>"something under your prefix may have changed; re-read it"</b>. That is the
 * whole contract, and it is narrow on purpose for three reasons:
 *
 * <ul>
 *   <li><b>It is the most any backend can honestly promise.</b> etcd watches are a reliable ordered stream
 *       from a revision and could support far more; ZooKeeper's tell you a node or its children changed and
 *       leave you to discover what. A polling fallback cannot see intermediate states at all. Promising the
 *       union would mean the SPI lying about two of the three.</li>
 *   <li><b>Consumers must be level-triggered anyway.</b> The supervisor's reconciliation loop has to be a
 *       pure function of observed state, so that it converges whether or not it was told — otherwise it
 *       never fixes anything that changed while it was down. A consumer built that way has no use for the
 *       payload, because it re-reads regardless.</li>
 *   <li><b>A lossy hint cannot be misused.</b> An API that hands over changed keys invites someone to
 *       apply just those, which is correct until the first missed or coalesced notification and then
 *       silently is not. Withholding the payload makes the safe design the only one available.</li>
 * </ul>
 *
 * <p>So notifications may be <b>coalesced</b> (several changes, one call), <b>spurious</b> (a call when
 * nothing relevant changed), and <b>missed entirely</b> if a backend's connection drops. None of that is a
 * bug. Treat a watch as a way to stop polling so often, never as a way to avoid re-reading.
 *
 * <h2>Threading</h2>
 *
 * <p>The callback runs on a thread owned by the backend or by the polling fallback, never the caller's. It
 * <b>must not block</b> and must not call back into the store: the usual body is one flag set or one
 * queue offer to wake a loop that does the real work.
 */
public interface Watch extends AutoCloseable {

    /** Stops notifications. Idempotent, and safe to call from a notification callback. */
    @Override
    void close();
}
