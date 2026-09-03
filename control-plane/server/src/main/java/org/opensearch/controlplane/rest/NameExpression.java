/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The comma-separated, {@code *}-wildcard name list OpenSearch accepts almost everywhere.
 *
 * <p>Index expressions, alias names, template names, pipeline IDs and setting-name filters all use the
 * same convention, so the matching lives here once rather than being reimplemented per resource. It had
 * been reimplemented four times before this existed, identically each time.
 *
 * <p><b>{@code *} is the only wildcard.</b> OpenSearch's {@code Regex.isSimpleMatchPattern} tests for
 * {@code *} alone, so {@code ?} is an ordinary character and an index literally named {@code a?b} is
 * matched by that name and not by {@code a*b} being clever. Everything outside the wildcards is quoted,
 * which is what keeps a name containing regex metacharacters from being interpreted as a pattern.
 *
 * <p>This is deliberately only the matching. What an empty list means, whether {@code _all} is special,
 * whether a leading {@code -} excludes, and whether a miss is an error are all per-resource decisions
 * that differ — see {@code IndexNameExpressionResolver} for how much they differ — and belong with the
 * resource rather than here.
 */
public final class NameExpression {

    private NameExpression() {}

    /** One glob as a regex. */
    public static Pattern toPattern(String glob) {
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

    /** A comma-separated list of globs, blank entries dropped. */
    public static List<Pattern> parse(String expression) {
        List<Pattern> patterns = new ArrayList<>();
        if (expression == null) {
            return patterns;
        }
        for (String token : expression.split(",")) {
            String name = token.trim();
            if (name.isEmpty() == false) {
                patterns.add(toPattern(name));
            }
        }
        return patterns;
    }

    public static boolean matchesAny(List<Pattern> patterns, String name) {
        return patterns.stream().anyMatch(pattern -> pattern.matcher(name).matches());
    }
}
