/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import java.util.Set;

/**
 * The registry used when no settings have been declared: everything is known and everything is
 * dynamic.
 *
 * <p>The default, and deliberately so. There is no accurate list to ship — see
 * {@link SettingsRegistry} — and accepting a setting the data plane will reject defers an error,
 * whereas rejecting one it would have accepted breaks a working deployment. Startup says which mode it
 * is in rather than leaving it to be discovered.
 */
public final class PermissiveSettings implements SettingsRegistry {

    @Override
    public boolean isKnown(Scope scope, String key) {
        return true;
    }

    @Override
    public Updateability updateabilityOf(Scope scope, String key) {
        return Updateability.DYNAMIC;
    }

    @Override
    public boolean isEnforcing() {
        return false;
    }

    @Override
    public Set<String> declaredKeys(Scope scope) {
        return Set.of();
    }

    @Override
    public String toString() {
        return "not enforcing (no settings registry declared; unknown settings are accepted)";
    }
}
