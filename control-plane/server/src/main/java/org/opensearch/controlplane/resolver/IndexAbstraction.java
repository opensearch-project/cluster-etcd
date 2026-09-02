/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.resolver;

import java.util.List;

/**
 * Something an index expression can name: a concrete index, or an alias standing for several.
 *
 * <p>OpenSearch calls this an abstraction because expressions do not distinguish the two — an alias
 * and an index are equally valid in the same position, and resolution flattens both to concrete index
 * names.
 *
 * @param name    the name the expression matches against
 * @param alias   whether this is an alias rather than a concrete index
 * @param indices the concrete indices this stands for; a single self-reference for a concrete index
 * @param hidden  excluded from wildcard expansion unless hidden expansion is on, or the pattern is
 *                itself dot-prefixed
 * @param open    concrete indices only; an alias reports {@code true}
 */
public record IndexAbstraction(String name, boolean alias, List<String> indices, boolean hidden, boolean open) {

    static IndexAbstraction index(String name, boolean hidden, boolean open) {
        return new IndexAbstraction(name, false, List.of(name), hidden, open);
    }

    static IndexAbstraction alias(String name, List<String> indices, boolean hidden) {
        return new IndexAbstraction(name, true, List.copyOf(indices), hidden, true);
    }
}
