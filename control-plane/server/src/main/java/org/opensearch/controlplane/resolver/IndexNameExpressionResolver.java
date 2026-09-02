/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.resolver;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Resolves an index expression to concrete index names.
 *
 * <p>Ported from OpenSearch's {@code IndexNameExpressionResolver}, reading from the key/value store
 * rather than cluster state. Every rule below was checked against OpenSearch 3.9.0, because several are
 * not what a reading of the API docs would suggest.
 *
 * <h2>Rules</h2>
 *
 * <ul>
 *   <li>Comma-separated expressions, each an index name, an alias, or a {@code *} pattern.
 *       {@code *} is the only wildcard — OpenSearch's {@code Regex.isSimpleMatchPattern} tests for
 *       {@code *} alone, so {@code ?} is a literal character.</li>
 *   <li>An empty list, {@code _all}, or exactly {@code *} means everything, subject to the expansion
 *       options.</li>
 *   <li><b>A leading {@code -} excludes, but only after a wildcard has appeared.</b> Verified:
 *       {@code cp-*,-cp-a} resolves to everything but {@code cp-a}, while {@code -cp-a} on its own is
 *       a missing index called {@code -cp-a} and 404s. Handling {@code -} unconditionally would be the
 *       obvious implementation and would be wrong.</li>
 *   <li>An expression matching an existing name exactly is taken as-is and never treated as a
 *       pattern, so an index literally named {@code a*b} is reachable.</li>
 *   <li><b>A leading {@code _} is a distinct error</b> — {@code invalid_index_name_exception}, not
 *       {@code index_not_found_exception} — because that prefix is reserved for API paths and a
 *       request reaching here means no API matched.</li>
 *   <li>Hidden indices are left out of wildcard expansion unless hidden expansion is on, <b>or the
 *       pattern is itself dot-prefixed</b>: {@code *} skips {@code .cp-hidden} while {@code .cp-*}
 *       finds it. Verified.</li>
 *   <li>Aliases flatten to their member indices.</li>
 *   <li>With neither open nor closed expansion — {@code expand_wildcards=none} — expressions are
 *       returned untouched rather than expanded.</li>
 * </ul>
 *
 * <p>Custom {@code ExpressionResolver} plugins are deliberately not modelled. The hook exists in
 * OpenSearch but nothing has ever implemented one, and it is a candidate for removal.
 *
 * <p>Date math expressions ({@code <logs-{now/d}>}) are <b>not</b> handled yet; such an expression is
 * treated as a literal name. That is a real gap rather than a decision.
 */
public final class IndexNameExpressionResolver {

    private static final String ALL = "_all";
    private static final String MATCH_ALL = "*";

    private final IndexAbstractions abstractions;

    public IndexNameExpressionResolver(IndexAbstractions abstractions) {
        this.abstractions = abstractions;
    }

    /**
     * Resolves a raw path parameter.
     *
     * @param expression comma-separated, or {@code null} for "everything"
     */
    public IndexResolution resolve(String expression, IndicesOptions options) {
        List<String> expressions = new ArrayList<>();
        if (expression != null && expression.isBlank() == false) {
            for (String token : expression.split(",")) {
                expressions.add(token.trim());
            }
        }
        return resolve(expressions, options);
    }

    public IndexResolution resolve(List<String> expressions, IndicesOptions options) {
        if (options.expandsWildcards() == false) {
            // expand_wildcards=none: nothing is expanded, so only literal names survive.
            return literalsOnly(expressions, options);
        }

        if (isEmptyOrTrivialWildcard(expressions)) {
            return IndexResolution.resolved(expandAll(options));
        }

        Set<String> result = new LinkedHashSet<>();
        boolean wildcardSeen = false;

        for (String raw : expressions) {
            if (raw.isEmpty()) {
                return IndexResolution.notFound(raw);
            }
            if (raw.charAt(0) == '_') {
                return IndexResolution.invalidName(raw, "must not start with '_'.");
            }

            // An exact match is never reinterpreted as a pattern.
            IndexAbstraction exact = abstractions.get(raw);
            if (exact != null) {
                result.addAll(exact.indices());
                continue;
            }

            boolean exclude = false;
            String expression = raw;
            if (expression.charAt(0) == '-' && wildcardSeen) {
                exclude = true;
                expression = expression.substring(1);
            }

            if (expression.indexOf('*') < 0) {
                // A concrete name that does not exist.
                IndexAbstraction named = abstractions.get(expression);
                if (named == null) {
                    if (exclude == false && options.ignoreUnavailable() == false) {
                        return IndexResolution.notFound(expression);
                    }
                    if (exclude) {
                        result.remove(expression);
                    }
                    continue;
                }
                if (exclude) {
                    named.indices().forEach(result::remove);
                } else {
                    result.addAll(named.indices());
                }
                continue;
            }

            wildcardSeen = true;
            Set<String> matched = expandPattern(expression, options);
            if (matched.isEmpty() && options.allowNoIndices() == false) {
                return IndexResolution.notFound(expression);
            }
            if (exclude) {
                result.removeAll(matched);
            } else {
                result.addAll(matched);
            }
        }

        return IndexResolution.resolved(List.copyOf(result));
    }

    /** {@code expand_wildcards=none}: names are taken literally, patterns match nothing. */
    private IndexResolution literalsOnly(List<String> expressions, IndicesOptions options) {
        Set<String> result = new LinkedHashSet<>();
        for (String raw : expressions) {
            if (raw.isEmpty()) {
                continue;
            }
            if (raw.charAt(0) == '_') {
                return IndexResolution.invalidName(raw, "must not start with '_'.");
            }
            IndexAbstraction abstraction = abstractions.get(raw);
            if (abstraction == null) {
                if (options.ignoreUnavailable() == false && raw.indexOf('*') < 0) {
                    return IndexResolution.notFound(raw);
                }
                continue;
            }
            result.addAll(abstraction.indices());
        }
        return IndexResolution.resolved(List.copyOf(result));
    }

    private boolean isEmptyOrTrivialWildcard(List<String> expressions) {
        if (expressions.isEmpty()) {
            return true;
        }
        return expressions.size() == 1
            && (ALL.equals(expressions.getFirst()) || MATCH_ALL.equals(expressions.getFirst()));
    }

    private List<String> expandAll(IndicesOptions options) {
        Set<String> result = new LinkedHashSet<>();
        for (IndexAbstraction abstraction : abstractions.all().values()) {
            if (abstraction.alias()) {
                continue;
            }
            if (abstraction.hidden() && options.expandHidden() == false) {
                continue;
            }
            if (stateAllowed(abstraction, options)) {
                result.add(abstraction.name());
            }
        }
        return List.copyOf(result);
    }

    private Set<String> expandPattern(String pattern, IndicesOptions options) {
        Pattern compiled = globToPattern(pattern);
        // A dot-prefixed pattern reaches dot-prefixed hidden indices without expand_wildcards=hidden.
        boolean includeHidden = options.expandHidden() || pattern.startsWith(".");

        Set<String> result = new LinkedHashSet<>();
        for (IndexAbstraction abstraction : abstractions.all().values()) {
            if (compiled.matcher(abstraction.name()).matches() == false) {
                continue;
            }
            if (abstraction.hidden() && includeHidden == false) {
                continue;
            }
            result.addAll(concreteNames(abstraction, options));
        }
        return result;
    }

    /**
     * Flattens to concrete index names, dropping any whose state the options exclude.
     *
     * <p>Used only for wildcard expansion. An explicitly named index or alias is <b>not</b> state
     * filtered: in OpenSearch the state check lives inside {@code expand()}, so naming a closed index
     * outright reaches it even under {@code expand_wildcards=open}. {@code expand_wildcards} governs
     * what a pattern expands to, not what an explicit name means.
     */
    private List<String> concreteNames(IndexAbstraction abstraction, IndicesOptions options) {
        List<String> names = new ArrayList<>(abstraction.indices().size());
        for (String index : abstraction.indices()) {
            IndexAbstraction concrete = abstractions.get(index);
            if (concrete == null || stateAllowed(concrete, options)) {
                names.add(index);
            }
        }
        return names;
    }

    private static boolean stateAllowed(IndexAbstraction abstraction, IndicesOptions options) {
        return abstraction.open() ? options.expandOpen() : options.expandClosed();
    }

    /** {@code *} is the only wildcard; everything else is literal. */
    private static Pattern globToPattern(String glob) {
        StringBuilder regex = new StringBuilder(glob.length() + 8);
        int literalStart = 0;
        for (int i = 0; i < glob.length(); i++) {
            if (glob.charAt(i) == '*') {
                if (i > literalStart) {
                    regex.append(Pattern.quote(glob.substring(literalStart, i)));
                }
                regex.append(".*");
                literalStart = i + 1;
            }
        }
        if (literalStart < glob.length()) {
            regex.append(Pattern.quote(glob.substring(literalStart)));
        }
        return Pattern.compile(regex.toString());
    }
}
