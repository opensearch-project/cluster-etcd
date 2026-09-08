/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import java.util.Optional;
import java.util.Set;

/**
 * What the data plane behind this control plane can actually do.
 *
 * <h2>Why this exists</h2>
 *
 * <p>The OpenSearch API specification describes the union of everything in the distribution,
 * including every bundled plugin. So "in the spec" does not mean "available in this deployment": a
 * pipeline using {@code normalization-processor} is spec-valid but a cluster without the
 * neural-search plugin rejects it with 400, and nothing in the spec marks that processor as
 * plugin-provided.
 *
 * <p>Left unchecked, the control plane accepts such a definition and the client discovers the problem
 * only when the pipeline runs. Rejecting it up front is a better experience, which means the control
 * plane has to be told what its data plane supports.
 *
 * <h2>Where the answer should come from</h2>
 *
 * <p>Ultimately from the data plane itself, not from a list maintained here. A cluster already reports
 * exactly this under {@code GET /_nodes} — {@code ingest.processors} and the three
 * {@code search_pipelines.*} lists — so the durable design is for the shard manager to publish those
 * capabilities into the data store for the control plane to read. Any list hand-maintained in this
 * repo would drift and cause false rejections, which are worse than the false acceptances they
 * replace.
 *
 * <p>Until that channel exists, the deployer declares capabilities in a file, generated from their own
 * cluster by {@code tools/dump-capabilities.sh}. Generated rather than written, for the same reason.
 * That is why this is an interface: swapping {@link DeclaredCapabilities} for a store-backed
 * implementation later changes one line of wiring.
 *
 * @see PermissiveCapabilities for the default when nothing is declared
 */
public interface DataPlaneCapabilities {

    /**
     * Whether the data plane supports a processor.
     *
     * @return {@code true} if supported, or if this implementation does not enforce
     */
    boolean supportsProcessor(ProcessorKind kind, String type);

    /**
     * Whether unsupported values are rejected at all.
     *
     * <p>Exposed so startup can say plainly which mode it is in. A control plane silently accepting
     * anything is a reasonable choice, but not a good surprise.
     */
    boolean isEnforcing();

    /** The declared set for a kind, for diagnostics. Empty when not enforcing. */
    Set<String> supportedProcessors(ProcessorKind kind);

    /**
     * The scripting languages the data plane has, if they were declared.
     *
     * <p>An {@link Optional} rather than an empty default, because "no languages" and "nobody said" are
     * different answers and the difference is visible: {@code get_script_languages} is a pure passthrough
     * of this data, so with nothing declared there is no operation to perform. Reporting empty lists
     * would tell a client it may use no language at all, which is worse than admitting we cannot say.
     */
    Optional<ScriptLanguages> scriptLanguages();
}
