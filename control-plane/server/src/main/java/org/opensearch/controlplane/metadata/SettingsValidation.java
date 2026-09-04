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
 * <p>Shared by four write paths that ask overlapping but distinct questions of the same data, and
 * report the answers in four different shapes. The distinctions are real and were each verified:
 *
 * <table border="1">
 *   <caption>Who rejects what</caption>
 *   <tr><th></th><th>unknown</th><th>private</th><th>static</th></tr>
 *   <tr><td>{@code indices.put_settings}</td><td>yes</td><td>yes</td><td><b>yes</b></td></tr>
 *   <tr><td>{@code cluster.put_settings}</td><td>yes</td><td>n/a</td><td><b>yes</b></td></tr>
 *   <tr><td>{@code indices.create}</td><td>yes</td><td>yes</td><td><b>no</b></td></tr>
 *   <tr><td>template puts</td><td>yes</td><td>yes</td><td><b>no</b></td></tr>
 * </table>
 *
 * <p>Create and the template puts permit static settings because they are <em>where static settings are
 * set</em> — {@code index.number_of_shards} is the obvious case. Only an update to something that
 * already exists has to care.
 *
 * <p>Does nothing when the registry is not enforcing — that is the default, and the whole point of it
 * being a default is that these call sites do not have to check.
 */
public final class SettingsValidation {

    /**
     * What was wrong, in the order a real cluster reports it: existence, then permission, then
     * updateability.
     *
     * <p>{@code notUserSettable} is kept apart from {@code unknown} because the three write paths report
     * it three different ways — see {@code ErrorResponses}. An earlier version folded the two together
     * on the strength of a probe that used {@code index.provided_name}, which is <em>absent</em> from
     * the registry rather than private, so it was really testing the unknown path. A registered private
     * setting has its own message.
     */
    public record Problems(
        List<String> unknown,
        List<String> notUserSettable,
        List<String> notDynamic,
        List<String> notEverUpdateable
    ) {

        public boolean isEmpty() {
            return unknown.isEmpty() && notUserSettable.isEmpty()
                && notDynamic.isEmpty() && notEverUpdateable.isEmpty();
        }

        /**
         * Static and final together, which is how an <em>open</em> index reports them.
         *
         * <p>A real cluster pre-filters everything non-dynamic before it reaches the code that
         * distinguishes final from static, so on an open index both answer
         * {@code Can't update non dynamic settings ...}. Only once every target index is closed does
         * final get its own message. Sorted, since the server interpolates a set.
         */
        public List<String> allNotDynamic() {
            return java.util.stream.Stream.concat(notDynamic.stream(), notEverUpdateable.stream())
                .sorted()
                .toList();
        }
    }

    private SettingsValidation() {}

    /**
     * @param requireUpdateable reject settings that exist but may not be changed after creation. False
     *                          for {@code indices.create} and the template puts, which are where static
     *                          and final settings are legitimately set.
     */
    public static Problems check(
        Settings settings,
        SettingsRegistry registry,
        Scope scope,
        boolean requireUpdateable
    ) {
        List<String> unknown = new ArrayList<>();
        List<String> notUserSettable = new ArrayList<>();
        List<String> notDynamic = new ArrayList<>();
        List<String> notEverUpdateable = new ArrayList<>();
        if (registry.isEnforcing() == false) {
            return new Problems(unknown, notUserSettable, notDynamic, notEverUpdateable);
        }
        for (String key : settings.keys()) {
            if (registry.isKnown(scope, key) == false) {
                unknown.add(key);
                continue;
            }
            Updateability updateability = registry.updateabilityOf(scope, key);
            if (updateability == Updateability.NOT_USER_SETTABLE) {
                notUserSettable.add(key);
            } else if (requireUpdateable && updateability == Updateability.STATIC) {
                notDynamic.add(key);
            } else if (requireUpdateable && updateability == Updateability.FINAL) {
                notEverUpdateable.add(key);
            }
        }
        return new Problems(unknown, notUserSettable, notDynamic, notEverUpdateable);
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
