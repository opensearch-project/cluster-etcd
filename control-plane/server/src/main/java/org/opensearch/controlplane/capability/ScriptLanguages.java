/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * The scripting languages the data plane has, and the contexts each may be used in.
 *
 * <p>Exactly what {@code GET /_script_language} reports, and the only reason this service can answer
 * that operation at all: which languages exist depends on installed modules — {@code painless} is core
 * while {@code expression} and {@code mustache} are modules — so it is a data plane fact, like processor
 * types.
 *
 * <p>It is the one capability of that kind with a first-class source. A cluster reports processors under
 * {@code GET /_nodes} and reports <b>nothing</b> about repository types anywhere; script languages have
 * their own endpoint, which is both where this comes from and the operation it lets us serve.
 *
 * @param typesAllowed        {@code inline}, {@code stored}, or both — which script *types* the cluster
 *                            permits, governed by the {@code script.allowed_types} setting
 * @param contextsByLanguage  language to the contexts it may run in, sorted by language
 */
public record ScriptLanguages(List<String> typesAllowed, NavigableMap<String, List<String>> contextsByLanguage) {

    public ScriptLanguages(List<String> typesAllowed, Map<String, List<String>> contextsByLanguage) {
        this(List.copyOf(typesAllowed), sorted(contextsByLanguage));
    }

    private static NavigableMap<String, List<String>> sorted(Map<String, List<String>> contexts) {
        NavigableMap<String, List<String>> copy = new TreeMap<>();
        contexts.forEach((language, names) -> copy.put(language, List.copyOf(names)));
        return copy;
    }

    /** Whether a stored script may declare this language. */
    public boolean supports(String language) {
        return contextsByLanguage.containsKey(language);
    }

    public Set<String> languages() {
        return contextsByLanguage.keySet();
    }
}
