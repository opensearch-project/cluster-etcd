/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.tenant.TenantId;

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
 * <p>This is where the task registrar and a clock will go when they are needed, which keeps those
 * additions from rippling through the routing and HTTP layers.
 */
public record OperationContext(TenantId tenant, KeyValueStore store) {}
