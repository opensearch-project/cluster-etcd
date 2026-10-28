/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import java.util.List;

/**
 * One column of a cat table: the name and aliases a client can ask for in {@code h} and {@code s}, what
 * {@code ?help} says about it, and how it lines up.
 *
 * @param byDefault shown when {@code h} is absent
 * @param right     right-aligned, as OpenSearch aligns numbers
 */
record Column(String name, List<String> aliases, String description, boolean right, boolean byDefault) {

    static Column left(String name, String aliases, String description) {
        return new Column(name, split(aliases), description, false, true);
    }

    static Column right(String name, String aliases, String description) {
        return new Column(name, split(aliases), description, true, true);
    }

    /** Not shown unless asked for with {@code h}. */
    Column optional() {
        return new Column(name, aliases, description, right, false);
    }

    private static List<String> split(String aliases) {
        return aliases.isEmpty() ? List.of() : List.of(aliases.split(","));
    }
}
