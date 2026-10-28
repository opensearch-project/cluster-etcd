/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.rest.TimeValues;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * What a client chooses about a cat table: its columns ({@code h}), its order ({@code s}), whether it has a
 * header ({@code v}), how times read ({@code time}), and whether it is text or JSON ({@code format}).
 * {@code RestTable}'s behaviour, checked against a node's output byte for byte.
 *
 * <p>The behaviours worth knowing, because a naive version gets them wrong:
 *
 * <ul>
 *   <li><b>Columns are named as typed.</b> {@code h=sh} produces a column headed {@code sh}, and in JSON the key
 *       is {@code sh}, not {@code shards}. An unknown name is silently dropped, and so are all of them if none
 *       is known.</li>
 *   <li><b>A name resolves before an alias, and the last column with an alias wins</b>: {@code t} is on both
 *       {@code epoch} and {@code shards} in {@code _cat/health}, so {@code h=t} is {@code shards}.</li>
 *   <li><b>The last column is not padded</b> when left-aligned, so lines carry no trailing spaces, except where
 *       the last cell is blank and the separator before it remains. Every line ends in a newline, the last one
 *       too, and an empty table without {@code v} is an empty body.</li>
 *   <li><b>Sorting compares values, not text</b>: a number numerically, a time by its length, a null first, and
 *       anything else by its text. Equal rows keep the order they had.</li>
 *   <li><b>JSON values are strings</b>, {@code "pri":"1"}, and a blank cell is {@code null}.</li>
 * </ul>
 *
 * <p>Where this differs from OpenSearch: an unknown {@code s} column is a {@code 400} where upstream throws an
 * {@code UnsupportedOperationException} and answers {@code 500}; padding is always exact, where upstream's
 * pad width is a {@code byte} that wraps for a gap of 128 or more; and {@code yaml}, {@code smile} and
 * {@code cbor} are refused rather than produced.
 */
final class CatRendering {

    static final String TEXT = "text/plain; charset=UTF-8";

    private CatRendering() {}

    // ------------------------------------------------------------------ help

    /** {@code ?help}: every column the table has, one line each, as {@code name | aliases | description}. */
    static RestResponse help(List<Column> columns) {
        int nameWidth = columns.stream().mapToInt(column -> column.name().length()).max().orElse(0);
        int aliasWidth = columns.stream().mapToInt(column -> String.join(",", column.aliases()).length()).max().orElse(0);
        int descriptionWidth = columns.stream().mapToInt(column -> column.description().length()).max().orElse(0);
        StringBuilder text = new StringBuilder();
        for (Column column : columns) {
            text.append(pad(column.name(), nameWidth)).append(" | ")
                .append(pad(String.join(",", column.aliases()), aliasWidth)).append(" | ")
                .append(pad(column.description(), descriptionWidth)).append('\n');
        }
        return text(text.toString());
    }

    // ------------------------------------------------------------------ the table

    /** One column as the client asked for it: the column itself, and the token that names it in the output. */
    private record Shown(String token, Column column, int index) {}

    static RestResponse render(RestRequest request, CatTable table) {
        List<Shown> shown = shownColumns(request.param("h"), table.columns());
        List<Object[]> rows = sorted(request.param("s"), table);

        if (isJson(request)) {
            ArrayNode array = Json.array();
            for (Object[] row : rows) {
                ObjectNode object = array.addObject();
                for (Shown each : shown) {
                    String cell = cell(row[each.index()], request);
                    if (cell == null) {
                        object.putNull(each.token());
                    } else {
                        object.put(each.token(), cell);
                    }
                }
            }
            return Json.response(RestStatus.OK, array, request);
        }

        boolean verbose = request.paramAsBoolean("v", false);
        String[][] cells = new String[rows.size()][];
        int[] widths = new int[shown.size()];
        for (int c = 0; c < shown.size(); c++) {
            widths[c] = verbose ? shown.get(c).token().length() : 0;
        }
        for (int r = 0; r < rows.size(); r++) {
            cells[r] = new String[shown.size()];
            for (int c = 0; c < shown.size(); c++) {
                String text = cell(rows.get(r)[shown.get(c).index()], request);
                cells[r][c] = text == null ? "" : text;
                widths[c] = Math.max(widths[c], cells[r][c].length());
            }
        }

        StringBuilder text = new StringBuilder();
        if (verbose) {
            appendLine(text, shown, widths, shown.stream().map(Shown::token).toArray(String[]::new));
        }
        for (String[] line : cells) {
            appendLine(text, shown, widths, line);
        }
        return text(text.toString());
    }

    private static void appendLine(StringBuilder text, List<Shown> shown, int[] widths, String[] line) {
        for (int c = 0; c < shown.size(); c++) {
            boolean last = c == shown.size() - 1;
            String value = line[c];
            if (shown.get(c).column().right()) {
                text.append(" ".repeat(widths[c] - value.length())).append(value);
            } else {
                text.append(value);
                if (last == false) {
                    text.append(" ".repeat(widths[c] - value.length()));
                }
            }
            if (last == false) {
                text.append(' ');
            }
        }
        text.append('\n');
    }

    // ------------------------------------------------------------------ columns: h

    private static List<Shown> shownColumns(String h, List<Column> columns) {
        Set<String> tokens = new LinkedHashSet<>();
        if (h == null) {
            columns.stream().filter(Column::byDefault).forEach(column -> tokens.add(column.name()));
        } else {
            for (String token : h.split(",")) {
                if (token.indexOf('*') >= 0) {
                    // A pattern expands against every name first, then against every alias, in table order.
                    Pattern pattern = glob(token);
                    columns.stream().filter(column -> pattern.matcher(column.name()).matches())
                        .forEach(column -> tokens.add(column.name()));
                    columns.stream().filter(column -> column.aliases().stream().anyMatch(alias -> pattern.matcher(alias).matches()))
                        .forEach(column -> tokens.add(column.name()));
                } else {
                    tokens.add(token);
                }
            }
        }
        List<Shown> shown = new ArrayList<>();
        for (String token : tokens) {
            int index = resolve(token, columns);
            if (index >= 0) {
                shown.add(new Shown(token, columns.get(index), index));
            }
        }
        return shown;
    }

    /** A column's position by exact name, else by alias with the last column to carry it winning; -1 if unknown. */
    private static int resolve(String token, List<Column> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(token)) {
                return i;
            }
        }
        int found = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).aliases().contains(token)) {
                found = i;
            }
        }
        return found;
    }

    /** {@code *} is the only wildcard, as {@code Regex.simpleMatch} reads it. */
    private static Pattern glob(String glob) {
        return Pattern.compile(Arrays.stream(glob.split("\\*", -1)).map(Pattern::quote).collect(Collectors.joining(".*")));
    }

    // ------------------------------------------------------------------ order: s

    private static List<Object[]> sorted(String s, CatTable table) {
        List<Object[]> rows = new ArrayList<>(table.rows());
        if (s == null || s.isEmpty()) {
            return rows;
        }
        // Names and aliases together, the last column to claim one winning, as RestTable's alias map does.
        Map<String, Integer> byName = new LinkedHashMap<>();
        for (int i = 0; i < table.columns().size(); i++) {
            byName.put(table.columns().get(i).name(), i);
            for (String alias : table.columns().get(i).aliases()) {
                byName.put(alias, i);
            }
        }
        List<Comparator<Object[]>> keys = new ArrayList<>();
        for (String key : s.split(",")) {
            boolean descending = key.endsWith(":desc");
            String name = descending ? key.substring(0, key.length() - 5) : key.endsWith(":asc") ? key.substring(0, key.length() - 4) : key;
            Integer index = byName.get(name);
            if (index == null) {
                throw new IllegalArgumentException("Unable to sort by unknown sort key `" + name + "`");
            }
            Comparator<Object[]> compare = (a, b) -> compareCells(a[index], b[index]);
            keys.add(descending ? compare.reversed() : compare);
        }
        rows.sort((a, b) -> {
            for (Comparator<Object[]> key : keys) {
                int result = key.compare(a, b);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        });
        return rows;
    }

    /** {@code RestTable.compare}: nulls first; the same comparable type by value; anything else by its text. */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static int compareCells(Object a, Object b) {
        if (a == null || b == null) {
            return a == null ? (b == null ? 0 : -1) : 1;
        }
        if (a.getClass() == b.getClass() && a instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    // ------------------------------------------------------------------ cells and format

    /** A cell as text, or null for a blank. A time reads in the unit {@code time} names, else as a time value. */
    private static String cell(Object value, RestRequest request) {
        if (value == null) {
            return null;
        }
        if (value instanceof Duration duration) {
            String unit = request.param("time");
            if (unit != null) {
                switch (unit.toLowerCase(Locale.ROOT)) {
                    case "nanos" -> { return Long.toString(duration.toNanos()); }
                    case "micros" -> { return Long.toString(duration.toNanos() / 1_000); }
                    case "ms" -> { return Long.toString(duration.toMillis()); }
                    case "s" -> { return Long.toString(duration.toSeconds()); }
                    case "m" -> { return Long.toString(duration.toMinutes()); }
                    case "h" -> { return Long.toString(duration.toHours()); }
                    case "d" -> { return Long.toString(duration.toDays()); }
                    default -> { }
                }
            }
            return TimeValues.format(duration);
        }
        return value.toString();
    }

    private static final Set<String> UNPRODUCIBLE = Set.of("yaml", "smile", "cbor");

    /**
     * {@code format} wins when present, and is {@code json} or text; anything else is text, as upstream reads it,
     * save the three binary and structured formats this service does not write. With no {@code format}, an
     * {@code Accept} of exactly {@code application/json} asks for JSON.
     */
    private static boolean isJson(RestRequest request) {
        String format = request.param("format");
        if (format != null) {
            String lower = format.toLowerCase(Locale.ROOT);
            if (UNPRODUCIBLE.contains(lower)) {
                throw new IllegalArgumentException("format [" + format + "] is not supported; use json or text");
            }
            return lower.equals("json");
        }
        String accept = request.header("accept");
        if (accept == null) {
            return false;
        }
        int parameters = accept.indexOf(';');
        return (parameters < 0 ? accept : accept.substring(0, parameters)).trim().equalsIgnoreCase("application/json");
    }

    // ------------------------------------------------------------------ small helpers

    private static RestResponse text(String body) {
        return new RestResponse(RestStatus.OK, TEXT, body.getBytes(StandardCharsets.UTF_8), Collections.emptyMap());
    }

    private static String pad(String value, int width) {
        return value + " ".repeat(Math.max(0, width - value.length()));
    }
}
