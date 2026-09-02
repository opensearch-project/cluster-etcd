/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import java.util.Set;

/**
 * Accepts everything.
 *
 * <p>The default when no capabilities file is supplied, and a legitimate choice for a deployer who
 * would rather let the data plane be the judge. It is not the better default on the merits — a client
 * gets a 200 for a pipeline that will never run — but it is the honest one here, because shipping a
 * guessed list of built-in processors would cause false rejections, and a false rejection breaks a
 * working deployment while a false acceptance only defers an error.
 *
 * <p>Startup says which mode it is in rather than leaving it implicit.
 */
public final class PermissiveCapabilities implements DataPlaneCapabilities {

    @Override
    public boolean supportsProcessor(ProcessorKind kind, String type) {
        return true;
    }

    @Override
    public boolean isEnforcing() {
        return false;
    }

    @Override
    public Set<String> supportedProcessors(ProcessorKind kind) {
        return Set.of();
    }

    @Override
    public String toString() {
        return "permissive (no capabilities declared; unsupported processors are accepted)";
    }
}
