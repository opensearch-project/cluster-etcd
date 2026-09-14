/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.store;

import org.opensearch.cli.SuppressForbidden;

/**
 * Prints the store backends a build or an installation can see.
 *
 * <p>D6.2 asks for a {@code --help}-equivalent diagnostic listing the schemes actually found, so that a
 * missing jar is distinguishable from a typo. A plugin has no command line, so this is the closest thing:
 * {@code ./gradlew :plugin:schemes} answers the question for a build, and the same text appears in the
 * error when a node cannot open its store.
 */
public final class StoreSchemes {

    private StoreSchemes() {}

    @SuppressForbidden(reason = "Allowed to print from #main()")
    public static void main(String[] args) {
        System.out.println(StoreConnection.describeInstalled());
    }
}
