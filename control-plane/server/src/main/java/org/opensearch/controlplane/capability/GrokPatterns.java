/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * The built-in grok pattern library the data plane has.
 *
 * <p>Declared data, from {@code GET /_ingest/processor/grok}. Which patterns exist is a property of the
 * {@code ingest-common} module, so it is a data plane fact like processor types rather than something to
 * hardcode here — and unlike processor types it is a fact the control plane only ever <em>reports</em>. No
 * validation uses it: a grok processor's pattern is checked by the node that runs it.
 *
 * <h2>Why the order is preserved</h2>
 *
 * <p>Because the endpoint has two modes and only one of them is sorted. The default answers
 * {@code Grok.BUILTIN_PATTERNS}, a {@link LinkedHashMap} in pattern-file load order, and {@code ?s=true}
 * answers a {@link TreeMap} of the same map. Verified against 3.9.0: the default starts at
 * {@code S3_REQUEST_LINE}, sorted starts at {@code BACULA_CAPACITY}.
 *
 * <p>So load order is what has to be stored. Sorting is recoverable from it and it is not recoverable
 * from sorted — an earlier version of the dump script sorted, and could therefore have served
 * {@code ?s=true} and never the default.
 *
 * @param patterns pattern name to pattern, in the data plane's load order
 */
public record GrokPatterns(Map<String, String> patterns) {

    public GrokPatterns(Map<String, String> patterns) {
        // LinkedHashMap, not Map.copyOf: the latter does not promise iteration order, and here it is the
        // whole point.
        this.patterns = Collections.unmodifiableMap(new LinkedHashMap<>(patterns));
    }

    /** As the data plane loaded them, which is what the default response reports. */
    public Map<String, String> inLoadOrder() {
        return patterns;
    }

    /** Sorted by name, which is what {@code ?s=true} reports. */
    public Map<String, String> sortedByName() {
        return new TreeMap<>(patterns);
    }

    /** Either ordering, chosen the way the request does. */
    public Map<String, String> ordered(boolean sorted) {
        return sorted ? sortedByName() : inLoadOrder();
    }

    public int size() {
        return patterns.size();
    }
}
