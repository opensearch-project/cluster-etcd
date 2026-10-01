/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import org.opensearch.controlplane.router.GeneratedRoutes;

/**
 * The identity this service reports from {@code GET /}.
 *
 * <p>Most of these fields describe an OpenSearch <em>node</em>, which this service is not. They are
 * reported anyway because the spec marks them required and because clients use them: language
 * clients gate features on {@code version.number}, and the spec repo's test harness refuses to run
 * at all until it can read {@code version.number} from {@code GET /}.
 *
 * <p>The values are therefore a claim about <b>which OpenSearch API this surface is compatible
 * with</b>, not a description of the running process. {@code lucene_version} in particular is
 * reported for compatibility even though nothing here embeds Lucene.
 *
 * @param nodeName     reported as {@code name}
 * @param clusterName  reported as {@code cluster_name}
 * @param version      the OpenSearch API version claimed; see {@link #DEFAULT_VERSION}
 */
public record ClusterIdentity(String nodeName, String clusterName, String version) {

    /**
     * The OpenSearch version this API surface claims to be.
     *
     * <p>Comes from {@code target_opensearch_version} in the scope file, by way of the generated
     * route table. That is deliberately the same value the codegen filters routes with, so the claim
     * and the served surface cannot drift apart: a service reporting 3.9.0 will not answer a route
     * that 3.9.0 does not have, and will not omit one that it does.
     *
     * <p>It is a real decision rather than a placeholder. Clients gate behaviour on it, and the spec
     * repo's test harness uses it to decide which stories apply — a story marked
     * {@code version: '>= 2.11'} is skipped if this is lower. Change it in the scope file and
     * regenerate; there is intentionally no runtime flag, because overriding it at startup would
     * recreate exactly the inconsistency this indirection removes.
     */
    public static final String DEFAULT_VERSION = GeneratedRoutes.TARGET_OPENSEARCH_VERSION;

    public static final String DISTRIBUTION = "opensearch";
    public static final String TAGLINE = "The OpenSearch Project: https://opensearch.org/";

    /** Honest rather than imitative: this is not a tar, deb, rpm, or docker build of OpenSearch. */
    public static final String BUILD_TYPE = "controlplane";

    /** Reported for compatibility. Nothing in this service embeds Lucene. */
    public static final String LUCENE_VERSION = "10.5.1";

    public static final String MINIMUM_WIRE_COMPATIBILITY_VERSION = "2.19.0";
    public static final String MINIMUM_INDEX_COMPATIBILITY_VERSION = "2.0.0";

    public static ClusterIdentity defaults() {
        return new ClusterIdentity("control-plane", "opensearch", DEFAULT_VERSION);
    }

    public ClusterIdentity withClusterName(String name) {
        return new ClusterIdentity(nodeName, name, version);
    }

    /**
     * Overrides the claimed version.
     *
     * <p>For tests only, and not exposed as a startup flag: reporting a version the route table was
     * not generated for is the inconsistency {@link #DEFAULT_VERSION} exists to prevent. Change
     * {@code target_opensearch_version} in the scope file instead.
     */
    public ClusterIdentity withVersion(String version) {
        return new ClusterIdentity(nodeName, clusterName, version);
    }
}
