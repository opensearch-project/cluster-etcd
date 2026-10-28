/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import java.util.List;

/**
 * The scripting contexts the data plane has, and the method signature each one exposes to a script.
 *
 * <p>Declared data, from {@code GET /_script_context}. The control plane cannot compute it: which
 * contexts exist depends on installed modules, and the signatures come from the classes those modules
 * register. Same family as {@link ScriptLanguages}, and read from the same file.
 *
 * <h2>Order is data, not presentation</h2>
 *
 * <p>All three lists here are ordered and are kept exactly as the capabilities file declares them,
 * because the response has to be byte-exact and none of the three orders is alphabetical:
 *
 * <ul>
 *   <li><b>Contexts</b> are sorted by name upstream, so this one happens to look alphabetical.</li>
 *   <li><b>Methods</b> are not. {@code ScriptContextInfo.toXContent} emits {@code execute} <em>first</em>
 *       and then the getters sorted among themselves, so a getter whose name sorts before the word
 *       "execute" would land in the wrong place under a plain sort.</li>
 *   <li><b>Parameters</b> are the method signature. Sorting them would be meaningless.</li>
 * </ul>
 *
 * @param contexts in declared order
 */
public record ScriptContexts(List<Context> contexts) {

    public ScriptContexts(List<Context> contexts) {
        this.contexts = List.copyOf(contexts);
    }

    /**
     * One scripting context.
     *
     * @param methods in declared order — {@code execute} first, then getters
     */
    public record Context(String name, List<Method> methods) {
        public Context(String name, List<Method> methods) {
            this.name = name;
            this.methods = List.copyOf(methods);
        }
    }

    /**
     * One method a context exposes.
     *
     * @param params in signature order
     */
    public record Method(String name, String returnType, List<Parameter> params) {
        public Method(String name, String returnType, List<Parameter> params) {
            this.name = name;
            this.returnType = returnType;
            this.params = List.copyOf(params);
        }
    }

    /**
     * One parameter of a method.
     *
     * <p><b>Component order matches the wire order</b>, which is {@code type} then {@code name} — the
     * reverse of how every other pair in this API reads, and of how the capabilities file spells it.
     * {@code ScriptContextInfo.ParameterInfo.toXContent} emits {@code type} first. Keeping the record in
     * that order is a small reminder at the one place it matters.
     */
    public record Parameter(String type, String name) {}

    /** How many contexts were declared, for diagnostics. */
    public int size() {
        return contexts.size();
    }
}
