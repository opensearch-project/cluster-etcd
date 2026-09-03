/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import java.util.Set;

/**
 * Which settings exist in the data plane, and what may be done with each.
 *
 * <h2>Why this exists</h2>
 *
 * <p>The same argument as {@link DataPlaneCapabilities}, applied to a registry that moves faster. A
 * real cluster rejects {@code {"settings":{"totally_bogus":1}}} with {@code settings_exception},
 * {@code unknown setting [index.totally_bogus]}, and rejects a static setting changed on an open index.
 * Neither check is possible without knowing what settings exist and which are dynamic.
 *
 * <p>That cannot be hardcoded here. There are 219 built-in index settings, OpenSearch adds more on a
 * semi-regular basis, and <b>plugins register their own</b> — so a list compiled into this repo is
 * wrong for some deployment on the day it ships. The error is asymmetric: a false rejection breaks a
 * working cluster, a false acceptance only defers an error to the data plane.
 *
 * <h2>Three kinds of entry, and why that matters</h2>
 *
 * <p>A flat list of keys cannot express the registry. OpenSearch splits registered settings into
 * ordinary keys and <em>complex matchers</em>, and two of the built-in index settings are the latter:
 * {@code Setting.groupSetting("index.analysis.", ...)} and the same for {@code index.similarity.}.
 * Anything beneath those prefixes is valid, without limit. Affix settings put the variable part in the
 * middle — {@code index.routing.allocation.include.*}. A registry that only knew exact keys would
 * reject every custom analyzer, which is exactly the false rejection this mechanism exists to avoid.
 *
 * <h2>Where the answer comes from</h2>
 *
 * <p>From the data plane, generated rather than written, by an endpoint whose addition
 * to OpenSearch is pending. As with capabilities, the durable answer is
 * the shard manager publishing this into the shared data store where it cannot go stale unnoticed,
 * which is why this is an interface: that is a change of implementation, not of design.
 *
 * @see PermissiveSettings for the default when nothing is declared
 */
public interface SettingsRegistry {

    /** What a setting permits, once it is known to exist. */
    enum Updateability {
        /** Changeable on a live index or cluster. */
        DYNAMIC,
        /** Fixed once set; {@code Setting.Property.Final}. */
        FINAL,
        /** {@code InternalIndex} or {@code PrivateIndex} — a user may not supply it at all. */
        NOT_USER_SETTABLE
    }

    /** The scopes a setting can be registered in. Kept separate because the two registries are. */
    enum Scope {
        INDEX,
        CLUSTER
    }

    /**
     * Whether a setting is registered at all.
     *
     * @return {@code true} if known, or if this implementation does not enforce
     */
    boolean isKnown(Scope scope, String key);

    /**
     * What may be done with a setting.
     *
     * <p>{@code DYNAMIC} for an unknown key when not enforcing, so a non-enforcing registry permits
     * everything rather than having to be special-cased at each call site.
     */
    Updateability updateabilityOf(Scope scope, String key);

    /**
     * Whether unknown or immutable settings are rejected at all.
     *
     * <p>Exposed so startup can say plainly which mode it is in. Silently accepting anything is a
     * reasonable default, but not a good surprise.
     */
    boolean isEnforcing();

    /** The declared keys and patterns for a scope, for diagnostics. Empty when not enforcing. */
    Set<String> declaredKeys(Scope scope);
}
