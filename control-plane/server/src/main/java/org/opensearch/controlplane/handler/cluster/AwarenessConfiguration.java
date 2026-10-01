/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.List;

/**
 * The allocation-awareness configuration, read out of the tenant's cluster settings.
 *
 * <p>Weighted routing is the first operation whose validation depends on the <em>value</em> of a cluster
 * setting rather than on its existence, so this is the first place the control plane reads its own
 * settings store as configuration. Two settings matter:
 *
 * <ul>
 *   <li>{@code cluster.routing.allocation.awareness.attributes} — which attribute names may carry
 *       weights at all. An attribute absent from this list is rejected outright.</li>
 *   <li>{@code cluster.routing.allocation.awareness.force.<attribute>.values} — the values that must
 *       each be given a weight. Verified against 3.9.0: with {@code force.zone.values: z1,z2,z3}, a
 *       {@code PUT} covering only {@code z1} and {@code z2} is rejected.</li>
 * </ul>
 *
 * <p>Transient overrides persistent, which is the precedence a real cluster applies when resolving a
 * setting. {@link ClusterSettingsStore} deliberately does not do that merge itself — it stores what was
 * set, in the scope it was set in — so a reader that needs the effective value asks for it here.
 *
 * <h2>Only settings this service was told about</h2>
 *
 * <p>A real cluster resolves these against node settings too, so a deployment that pins
 * {@code cluster.routing.allocation.awareness.attributes} in {@code opensearch.yml} has awareness
 * configured without any cluster-settings write. This service cannot see that: the settings registry
 * reports which settings <em>exist</em> and how they behave, not what any node has them set to.
 *
 * <p>The consequence is worth being plain about, because it is a behavioural difference and not merely a
 * missing feature: <b>weighted routing requires the awareness attributes to have been set through this
 * service.</b> Setting them only in {@code opensearch.yml} leaves every {@code PUT} and {@code GET}
 * rejected as an invalid awareness attribute. The alternative — accepting any attribute name — trades a
 * visible rejection for weights that are silently never applied, which is the worse failure. Recorded
 * under considered divergences in the README.
 */
record AwarenessConfiguration(List<String> attributes, Settings effective) {

    private static final String ATTRIBUTES = "cluster.routing.allocation.awareness.attributes";
    private static final String FORCE_PREFIX = "cluster.routing.allocation.awareness.force.";
    private static final String FORCE_SUFFIX = ".values";

    static AwarenessConfiguration read(KeyValueStore store) {
        ClusterSettingsStore.Current current = new ClusterSettingsStore(store).read();
        Settings effective = current.persistent().mergedWith(current.transientSettings());
        return new AwarenessConfiguration(effective.getAsList(ATTRIBUTES), effective);
    }

    /** Whether weights may be set for this attribute at all. */
    boolean allows(String attribute) {
        return attributes.contains(attribute);
    }

    /**
     * The forced awareness values for an attribute, each of which must be given a weight.
     *
     * <p>Empty when the group is not configured, which is the common case and means the only constraint
     * on the value set is the one this service cannot check — see
     * {@code WeightedRoutingHandlers.Put}.
     */
    List<String> forcedValues(String attribute) {
        return effective.getAsList(FORCE_PREFIX + attribute + FORCE_SUFFIX);
    }
}
