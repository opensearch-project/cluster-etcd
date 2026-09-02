/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import org.opensearch.controlplane.capability.DataPlaneCapabilities;
import org.opensearch.controlplane.capability.PermissiveCapabilities;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.handler.core.InfoHandler;
import org.opensearch.controlplane.handler.core.PingHandler;
import org.opensearch.controlplane.handler.indices.CreateIndexHandler;
import org.opensearch.controlplane.handler.indices.DeleteIndexHandler;
import org.opensearch.controlplane.handler.indices.ExistsIndexHandler;
import org.opensearch.controlplane.handler.pipeline.DeletePipelineHandler;
import org.opensearch.controlplane.handler.pipeline.GetPipelineHandler;
import org.opensearch.controlplane.handler.pipeline.PipelineKind;
import org.opensearch.controlplane.handler.pipeline.PutPipelineHandler;

/**
 * The single place where implemented operations are registered.
 *
 * <p>Registration is explicit rather than classpath-scanned or {@code ServiceLoader}-based. That is
 * deliberate: the list is greppable, the startup cost is nil, and a typo in an operation group name
 * fails immediately at construction rather than producing a handler that silently never receives a
 * request.
 *
 * <p>To implement an operation, add a class under {@code handler.<namespace>} mirroring the spec
 * namespace, then register it here.
 *
 * <p>Handlers take no constructor arguments. Everything they need arrives per-request in an
 * {@link OperationContext}, because the store they get is confined to the requesting tenant. When
 * further collaborators are needed — a task registrar, a clock — add them to
 * {@code OperationContext} rather than to constructors here, which keeps them out of the routing and
 * HTTP layers.
 */
public final class ControlPlaneHandlers {

    private ControlPlaneHandlers() {}

    /** Every operation group stubbed with a 501, then overridden by {@link #registerAll}. */
    public static HandlerRegistry build() {
        return build(ClusterIdentity.defaults());
    }

    public static HandlerRegistry build(ClusterIdentity identity) {
        return build(identity, new PermissiveCapabilities());
    }

    public static HandlerRegistry build(ClusterIdentity identity, DataPlaneCapabilities capabilities) {
        return registerAll(HandlerRegistry.withAllStubbed(), identity, capabilities);
    }

    public static HandlerRegistry registerAll(HandlerRegistry registry) {
        return registerAll(registry, ClusterIdentity.defaults(), new PermissiveCapabilities());
    }

    /**
     * Registers every implemented operation. Add new handlers here.
     *
     * <p>Anything not registered keeps its {@link NotImplementedHandler} stub, so the routing surface
     * stays complete no matter how little is implemented.
     */
    public static HandlerRegistry registerAll(
        HandlerRegistry registry,
        ClusterIdentity identity,
        DataPlaneCapabilities capabilities
    ) {
        return registry
            // _core
            .register(new InfoHandler(identity))
            .register(new PingHandler())
            // indices
            .register(new CreateIndexHandler())
            .register(new ExistsIndexHandler())
            .register(new DeleteIndexHandler())
            // ingest and search pipelines are the same API twice; see PipelineKind
            .register(new PutPipelineHandler(PipelineKind.ingest(), capabilities))
            .register(new GetPipelineHandler(PipelineKind.ingest()))
            .register(new DeletePipelineHandler(PipelineKind.ingest()))
            .register(new PutPipelineHandler(PipelineKind.search(), capabilities))
            .register(new GetPipelineHandler(PipelineKind.search()))
            .register(new DeletePipelineHandler(PipelineKind.search()));
    }
}
