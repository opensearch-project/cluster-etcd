/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An OpenSearch release version, {@code major.minor.revision}, as a data node reports it in {@code GET /_nodes}.
 *
 * <p>This service has no OpenSearch {@code Version} class to lean on, and needs only three things from one: to
 * compare two, to write one as a string, and to render one the way {@code index.version.created} appears in an
 * index's settings. A qualifier such as {@code -SNAPSHOT} is accepted and dropped: {@code _nodes} reports none,
 * and the id upstream encodes carries none.
 */
public record NodeVersion(int major, int minor, int revision) implements Comparable<NodeVersion> {

    private static final Pattern FORMAT = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(-[A-Za-z0-9.]+)?");

    /** {@code Version.MASK}, which upstream XORs into the id of every version from 3.0 on. */
    private static final int MASK = 0x08000000;

    /** @throws IllegalArgumentException if the text is not a version */
    public static NodeVersion parse(String text) {
        Matcher matcher = FORMAT.matcher(text == null ? "" : text.trim());
        if (matcher.matches() == false) {
            throw new IllegalArgumentException("not an OpenSearch version: [" + text + "]");
        }
        return new NodeVersion(
            Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)), Integer.parseInt(matcher.group(3)));
    }

    /**
     * {@code Version.id}, as it appears in an index's {@code index.version.created}: the release's id
     * ({@code major * 1000000 + minor * 10000 + revision * 100 + 99}, the {@code 99} marking a release build),
     * XOR {@link #MASK}. So 3.10.0 is {@code 137317827}.
     */
    public int encodedId() {
        return (major * 1_000_000 + minor * 10_000 + revision * 100 + 99) ^ MASK;
    }

    @Override
    public int compareTo(NodeVersion other) {
        int byMajor = Integer.compare(major, other.major);
        if (byMajor != 0) {
            return byMajor;
        }
        int byMinor = Integer.compare(minor, other.minor);
        return byMinor != 0 ? byMinor : Integer.compare(revision, other.revision);
    }

    @Override
    public String toString() {
        return major + "." + minor + "." + revision;
    }
}
