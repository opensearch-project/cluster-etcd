/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.metadata;

import org.opensearch.controlplane.capability.SettingsRegistry;
import org.opensearch.controlplane.capability.SettingsRegistry.Scope;
import org.opensearch.controlplane.capability.SettingsRegistry.Updateability;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Checks a settings document against the registry: does every key exist, and may every key be written.
 *
 * <p>Shared by the index and cluster write paths, which ask slightly different questions of the same
 * data. Both reject an unknown key; only {@code indices.put_settings} rejects a <em>static</em> one,
 * because {@code indices.create} is where static settings are legitimately set.
 *
 * <p>Does nothing when the registry is not enforcing — that is the default, and the whole point of it
 * being a default is that these call sites do not have to check.
 */
public final class SettingsValidation {

    /** What was wrong, in the order a real cluster reports it: existence first, then updateability. */
    public record Problems(List<String> unknown, List<String> notDynamic) {

        public boolean isEmpty() {
            return unknown.isEmpty() && notDynamic.isEmpty();
        }
    }

    private SettingsValidation() {}

    /**
     * @param requireDynamic reject settings that exist but may not be changed after creation
     */
    public static Problems check(
        Settings settings,
        SettingsRegistry registry,
        Scope scope,
        boolean requireDynamic
    ) {
        List<String> unknown = new ArrayList<>();
        List<String> notDynamic = new ArrayList<>();
        if (registry.isEnforcing() == false) {
            return new Problems(unknown, notDynamic);
        }
        for (String key : settings.keys()) {
            Updateability updateability = registry.updateabilityOf(scope, key);
            // A setting a user may not supply reads as unknown on the wire. Verified against 3.9.0:
            // PUT /i/_settings {"index":{"provided_name":"x"}} answers `unknown setting
            // [index.provided_name]`, the same message a genuinely unregistered key gets.
            if (registry.isKnown(scope, key) == false || updateability == Updateability.NOT_USER_SETTABLE) {
                unknown.add(key);
            } else if (requireDynamic && updateability != Updateability.DYNAMIC) {
                notDynamic.add(key);
            }
        }
        return new Problems(unknown, notDynamic);
    }

    /**
     * The {@code did you mean} suffix a real cluster appends to an unknown setting, or empty when it
     * would append the generic advice instead.
     *
     * <p>Ported from {@code AbstractScopedSettings.validate}: score every <em>exact</em> registered key
     * by Lucene's normalised Levenshtein similarity, keep those above {@code 0.7}, and order by
     * descending score. Worth reproducing rather than always emitting the generic branch, because a
     * typo is the common way to reach this message and the suggestion is the useful half of it.
     *
     * <p>One caveat: OpenSearch sorts with a stable sort over a {@code HashMap}'s iteration order, so
     * equally-scored candidates come out in an order that cannot be reproduced. Ties are broken by name
     * here, which is deterministic and differs from the server only when two keys score identically.
     */
    public static Optional<String> didYouMean(String key, Set<String> exactKeys) {
        record Scored(String name, float score) {}

        List<String> candidates = exactKeys.stream()
            .map(candidate -> new Scored(candidate, similarity(key, candidate)))
            .filter(scored -> scored.score() > 0.7f)
            .sorted(Comparator.comparingDouble(Scored::score).reversed().thenComparing(Scored::name))
            .map(Scored::name)
            .toList();

        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(candidates.size() == 1
            ? "[" + candidates.getFirst() + "]"
            : "any of " + candidates);
    }

    /**
     * Lucene's {@code LevenshteinDistance.getDistance}, which is what OpenSearch scores with.
     *
     * <p>Normalised by the <em>longer</em> of the two lengths — {@code lucene-suggest} 10.4.0 line 101,
     * {@code 1.0f - ((float) p[n] / Math.max(other.length(), sa.length))}. Worth stating because
     * dividing by the shorter length instead is a plausible-looking mistake that scores every
     * candidate too low, and the symptom is subtle: the suggestion list silently loses its tail. It
     * cost one differential run to catch, where a real cluster suggested two settings and this
     * suggested one.
     */
    static float similarity(String a, String b) {
        int n = a.length();
        int m = b.length();
        if (n == 0) {
            return m == 0 ? 1.0f : 0.0f;
        }
        if (m == 0) {
            return 0.0f;
        }

        int[] previous = new int[n + 1];
        int[] current = new int[n + 1];
        for (int i = 0; i <= n; i++) {
            previous[i] = i;
        }
        for (int j = 1; j <= m; j++) {
            current[0] = j;
            char bj = b.charAt(j - 1);
            for (int i = 1; i <= n; i++) {
                int cost = a.charAt(i - 1) == bj ? 0 : 1;
                current[i] = Math.min(Math.min(current[i - 1] + 1, previous[i] + 1), previous[i - 1] + cost);
            }
            int[] swap = previous;
            previous = current;
            current = swap;
        }
        return 1.0f - ((float) previous[n] / Math.max(n, m));
    }
}
