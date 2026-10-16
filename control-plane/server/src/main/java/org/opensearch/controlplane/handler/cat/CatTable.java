/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Columns and rows, before any client has chosen how to see them.
 *
 * <p>A cell is the value itself rather than text, because {@code s} sorts on the value as OpenSearch does: a
 * number numerically, a {@link java.time.Duration} by length, anything else by its text. {@code null} is a cell
 * with nothing to show, rendered blank; a value this service does not have is the string {@code x}, which is
 * what OpenSearch writes where it cannot say either.
 */
final class CatTable {

    /** What a cell holds when the value exists but this service cannot supply it. */
    static final String UNKNOWN = "x";

    private final List<Column> columns;
    private final List<Object[]> rows = new ArrayList<>();

    CatTable(List<Column> columns) {
        this.columns = List.copyOf(columns);
    }

    List<Column> columns() {
        return columns;
    }

    List<Object[]> rows() {
        return rows;
    }

    /** One row, a cell for every column in the order they were declared. */
    void row(Object... cells) {
        if (cells.length != columns.size()) {
            throw new IllegalArgumentException("row has " + cells.length + " cells for " + columns.size() + " columns");
        }
        rows.add(cells);
    }

    /** The same rows without the named columns, as {@code ts=false} removes {@code epoch} and {@code timestamp}. */
    CatTable without(Set<String> names) {
        List<Integer> kept = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (names.contains(columns.get(i).name()) == false) {
                kept.add(i);
            }
        }
        CatTable narrowed = new CatTable(kept.stream().map(columns::get).toList());
        for (Object[] row : rows) {
            narrowed.rows.add(kept.stream().map(i -> row[i]).toArray());
        }
        return narrowed;
    }

    @Override
    public String toString() {
        return columns.size() + " columns, " + rows.size() + " rows: " + rows.stream().map(Arrays::toString).toList();
    }
}
