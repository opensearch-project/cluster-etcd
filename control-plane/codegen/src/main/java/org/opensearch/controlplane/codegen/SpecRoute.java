/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.codegen;

import java.util.Set;

/**
 * One HTTP route discovered in the OpenAPI spec.
 *
 * <p>Note that several routes routinely share an {@code operationGroup}: the
 * OpenSearch spec models {@code indices.put_alias} as eleven distinct
 * (method, path) pairs, all of which are the same logical operation. The
 * generated route table keeps them separate; the handler registry collapses
 * them back onto a single handler.
 */
record SpecRoute(
    String method,
    String path,
    String operationGroup,
    String namespace,
    boolean deprecated,
    boolean ignorable,
    /** Whether this operation exists at the target OpenSearch version. */
    boolean availableAtTargetVersion,
    Set<String> queryParams
) implements Comparable<SpecRoute> {

    @Override
    public int compareTo(SpecRoute other) {
        int byGroup = operationGroup.compareTo(other.operationGroup);
        if (byGroup != 0) {
            return byGroup;
        }
        int byPath = path.compareTo(other.path);
        return byPath != 0 ? byPath : method.compareTo(other.method);
    }
}
