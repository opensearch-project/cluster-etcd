/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Build provenance for {@code GET /}, read from a properties file the build writes onto the
 * classpath.
 *
 * <p>Falls back to {@code unknown} when the file is absent, which is the case when running from an
 * IDE or from a source tree that was compiled directly. Reporting {@code unknown} is better than
 * fabricating a hash: {@code build_hash} exists so someone can identify exactly what is running, and
 * a made-up value defeats that.
 */
final class BuildInfo {

    private static final String RESOURCE = "/control-plane-build.properties";
    private static final String UNKNOWN = "unknown";

    private static final String HASH;
    private static final String DATE;
    private static final boolean SNAPSHOT;

    static {
        Properties properties = new Properties();
        try (InputStream in = BuildInfo.class.getResourceAsStream(RESOURCE)) {
            if (in != null) {
                properties.load(in);
            }
        } catch (IOException e) {
            // Fall through to the unknown defaults.
        }
        HASH = properties.getProperty("build.hash", UNKNOWN);
        DATE = properties.getProperty("build.date", UNKNOWN);
        SNAPSHOT = Boolean.parseBoolean(properties.getProperty("build.snapshot", "true"));
    }

    private BuildInfo() {}

    static String hash() {
        return HASH;
    }

    static String date() {
        return DATE;
    }

    static boolean isSnapshot() {
        return SNAPSHOT;
    }
}
