/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.tenant.TenantId;

import java.time.Clock;

/**
 * Per-request collaborators for a handler.
 *
 * <p>Storage arrives per-request rather than per-handler because tenancy is a property of the
 * request. {@link #store()} is already confined to {@link #tenant()}, so a handler cannot reach
 * another tenant's data even by accident.
 *
 * <p>{@link #tenant()} is exposed for logging, metrics, and task records — not for key construction.
 * Handlers should build keys as if they were the only tenant; the scoping is
 * {@link org.opensearch.controlplane.store.TenantScopedKeyValueStore}'s job.
 *
 * <p>{@link #clock()} is injected rather than read from {@code System.currentTimeMillis()} so that
 * anything stamping a timestamp — index creation dates, task submission times — is deterministic
 * under test.
 *
 * <p>This is where a task registrar will go when the task lifecycle grows beyond writing a record,
 * which keeps that addition from rippling through the routing and HTTP layers.
 */
public record OperationContext(TenantId tenant, KeyValueStore store, Clock clock) {

    /** Convenience for callers that do not care about time. */
    public OperationContext(TenantId tenant, KeyValueStore store) {
        this(tenant, store, Clock.systemUTC());
    }

    public long nowEpochMillis() {
        return clock.millis();
    }
}
