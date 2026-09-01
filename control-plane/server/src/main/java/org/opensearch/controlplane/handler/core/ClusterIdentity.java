/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

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
     * The OpenSearch version this API surface claims compatibility with.
     *
     * <p>This is a real decision, not a placeholder. Clients gate behaviour on it, and the test
     * harness uses it to decide which stories apply — a story marked {@code version: '>= 2.11'} is
     * skipped if this is lower. Set it too high and clients will expect operations that are still
     * stubbed; too low and they will avoid ones that work.
     *
     * <p>Worth knowing: the route table is generated from the spec bundle <em>without</em> a
     * {@code --opensearch-version} filter, so it is the union across versions and may include a
     * route that a real cluster at this version no longer serves. Passing a target version to the
     * merger would make the two consistent.
     */
    public static final String DEFAULT_VERSION = "3.9.0";

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

    public ClusterIdentity withVersion(String version) {
        return new ClusterIdentity(nodeName, clusterName, version);
    }
}
