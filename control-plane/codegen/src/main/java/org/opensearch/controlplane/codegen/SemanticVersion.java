/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.codegen;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Just enough semantic version handling to evaluate {@code x-version-added} and
 * {@code x-version-removed}.
 *
 * <p>Mirrors what the spec repo does with {@code semver.coerce}: take the leading numeric
 * components, default missing ones to zero, and discard any pre-release or build suffix. The spec
 * mixes two- and three-component forms — {@code '2.11'} alongside {@code 2.10.0} — so coercion is
 * not optional, and a plain string comparison would order {@code 2.9} after {@code 2.11}.
 *
 * <p>Dropping the pre-release suffix matters for the target version rather than the spec: a build
 * calling itself {@code 3.9.0-SNAPSHOT} must compare equal to {@code 3.9.0}, not sort before it.
 */
record SemanticVersion(int major, int minor, int patch) implements Comparable<SemanticVersion> {

    private static final Pattern LEADING_VERSION = Pattern.compile("^\\D*?(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?");

    /**
     * Parses a spec or configuration version string.
     *
     * @throws IllegalArgumentException if no leading numeric component can be found
     */
    static SemanticVersion parse(String raw) {
        Objects.requireNonNull(raw, "version");
        Matcher matcher = LEADING_VERSION.matcher(raw.trim());
        if (matcher.find() == false) {
            throw new IllegalArgumentException("cannot read a version number from [" + raw + "]");
        }
        return new SemanticVersion(
            Integer.parseInt(matcher.group(1)),
            matcher.group(2) == null ? 0 : Integer.parseInt(matcher.group(2)),
            matcher.group(3) == null ? 0 : Integer.parseInt(matcher.group(3))
        );
    }

    @Override
    public int compareTo(SemanticVersion other) {
        int byMajor = Integer.compare(major, other.major);
        if (byMajor != 0) {
            return byMajor;
        }
        int byMinor = Integer.compare(minor, other.minor);
        return byMinor != 0 ? byMinor : Integer.compare(patch, other.patch);
    }

    boolean isAtLeast(SemanticVersion other) {
        return compareTo(other) >= 0;
    }

    boolean isBefore(SemanticVersion other) {
        return compareTo(other) < 0;
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + patch;
    }
}
