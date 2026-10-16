/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.rest.TimeValues;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The table machinery on its own. The expected text is what a node prints for the same shape of table: left
 * columns padded on the right except the last, right columns padded on the left, one space between columns, and
 * a newline after every line.
 */
class CatRenderingTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Two aliases collide on {@code t}, as they do in {@code _cat/health}. */
    private static final List<Column> COLUMNS = List.of(
        Column.left("name", "n", "the name"),
        Column.right("count", "c,t", "how many"),
        Column.right("total", "t", "the total"),
        Column.left("note", "", "a note"),
        Column.left("hidden", "hd", "off by default").optional()
    );

    private static CatTable table() {
        CatTable table = new CatTable(COLUMNS);
        table.row("alpha", 5, 100, "first", "h1");
        table.row("b", 120, 7, null, "h2");
        table.row("gamma", 7, 7, "third", null);
        return table;
    }

    private static RestRequest request(String... params) {
        return request(Map.of(), params);
    }

    private static RestRequest request(Map<String, String> headers, String... params) {
        Map<String, List<String>> query = new LinkedHashMap<>();
        for (int i = 0; i < params.length; i += 2) {
            query.put(params[i], List.of(params[i + 1]));
        }
        return new RestRequest("GET", "/_cat/x", "/_cat/x", query, Map.of(), headers, new byte[0], "cat.x");
    }

    private static String text(RestResponse response) {
        assertEquals(RestStatus.OK, response.status());
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    private static String render(CatTable table, String... params) {
        return text(CatRendering.render(request(params), table));
    }

    // ------------------------------------------------------------------ text layout

    @Test
    void defaultColumnsHaveNoHeaderAndEveryLineEndsInANewline() {
        assertEquals(
            "alpha   5 100 first\n"
                + "b     120   7 \n"
                + "gamma   7   7 third\n",
            render(table()));
    }

    /** A blank last cell leaves the separator before it, so the line ends in a space, as upstream's does. */
    @Test
    void aBlankLastCellLeavesTheTrailingSeparator() {
        assertTrue(render(table()).contains("b     120   7 \n"));
    }

    @Test
    void verboseAddsTheHeaderAndWidensColumnsToIt() {
        assertEquals(
            "name  count total note\n"
                + "alpha     5   100 first\n"
                + "b       120     7 \n"
                + "gamma     7     7 third\n",
            render(table(), "v", "true"));
    }

    @Test
    void anEmptyTableIsAnEmptyBodyUnlessVerbose() {
        assertEquals("", render(new CatTable(COLUMNS)));
        assertEquals("name count total note\n", render(new CatTable(COLUMNS), "v", "true"));
    }

    // ------------------------------------------------------------------ h

    @Test
    void hChoosesColumnsAndTheirOrderAndNamesThemAsTyped() {
        assertEquals(
            "note  n\n"
                + "first alpha\n"
                + "      b\n"
                + "third gamma\n",
            render(table(), "h", "note,n", "v", "true"));
    }

    @Test
    void aNameBeatsAnAliasAndTheLastColumnWithAnAliasWins() {
        // "t" is an alias of both count and total, so it is total: the later column.
        assertEquals("100\n  7\n  7\n", render(table(), "h", "t"));
        // "count" is a name, and a name is resolved first.
        assertEquals("  5\n120\n  7\n", render(table(), "h", "count"));
    }

    @Test
    void anUnknownColumnIsDroppedAndNoKnownColumnLeavesBlankLines() {
        assertEquals("alpha\nb\ngamma\n", render(table(), "h", "name,nonesuch"));
        assertEquals("\n\n\n", render(table(), "h", "nonesuch"));
    }

    @Test
    void aWildcardExpandsAgainstNamesThenAliasesInTableOrder() {
        // n* matches the names "name" and "note", and the alias "n", which is name again and not repeated.
        assertEquals(
            "name  note\n"
                + "alpha first\n"
                + "b     \n"
                + "gamma third\n",
            render(table(), "h", "n*", "v", "true"));
        // h* matches the optional column's name, which an absent h would not have shown, and its alias.
        assertEquals("h1\nh2\n\n", render(table(), "h", "h*"));
    }

    @Test
    void anOptionalColumnAppearsOnlyWhenAskedFor() {
        assertTrue(render(table(), "v", "true").startsWith("name  count total note\n"));
        assertEquals("hidden\nh1\nh2\n\n", render(table(), "h", "hidden", "v", "true"));
    }

    // ------------------------------------------------------------------ s

    @Test
    void sortingCompareValuesNotText() {
        // As text "120" < "5" < "7"; as numbers 5 < 7 < 120.
        assertEquals("alpha\ngamma\nb\n", render(table(), "h", "name", "s", "count"));
        assertEquals("b\ngamma\nalpha\n", render(table(), "h", "name", "s", "count:desc"));
    }

    @Test
    void severalKeysAndStability() {
        // total ties between b and gamma (7); count then decides.
        assertEquals("gamma\nb\nalpha\n", render(table(), "h", "name", "s", "total,count"));
        assertEquals("alpha\ngamma\nb\n", render(table(), "h", "name", "s", "total:desc,count"));
        // Equal keys keep the order the rows had.
        assertEquals("b\ngamma\nalpha\n", render(table(), "h", "name", "s", "total"));
    }

    @Test
    void aNullSortsBeforeEverythingElse() {
        assertEquals("b\nalpha\ngamma\n", render(table(), "h", "name", "s", "note"));
    }

    @Test
    void sortingByAnAliasOrByAColumnNotShownWorks() {
        assertEquals("gamma\nb\nalpha\n", render(table(), "h", "name", "s", "n:desc"));
        assertEquals("alpha\ngamma\nb\n", render(table(), "h", "name", "s", "c"));
        // hidden is not shown, and holds null for gamma, which sorts first.
        assertEquals("gamma\nalpha\nb\n", render(table(), "h", "name", "s", "hidden"));
    }

    @Test
    void anUnknownSortKeyIsRejectedWithUpstreamsWords() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> render(table(), "s", "nonesuch"));
        assertEquals("Unable to sort by unknown sort key `nonesuch`", e.getMessage());
    }

    @Test
    void anUnknownKeyIsRejectedEvenForAnEmptyTable() {
        assertThrows(IllegalArgumentException.class, () -> render(new CatTable(COLUMNS), "s", "nonesuch"));
    }

    // ------------------------------------------------------------------ help

    @Test
    void helpListsEveryColumnWithItsAliasesAndDescription() {
        // Each column padded to the widest in its field, the description too, so every line ends in spaces.
        String line = "%-6s | %-3s | %-14s\n";
        assertEquals(
            String.format(line, "name", "n", "the name")
                + String.format(line, "count", "c,t", "how many")
                + String.format(line, "total", "t", "the total")
                + String.format(line, "note", "", "a note")
                + String.format(line, "hidden", "hd", "off by default"),
            text(CatRendering.help(COLUMNS)));
    }

    // ------------------------------------------------------------------ json and format

    @Test
    void jsonIsAnArrayOfObjectsOfStringsWithNullForBlank() throws Exception {
        JsonNode json = MAPPER.readTree(CatRendering.render(request("format", "json", "h", "name,count,note"), table()).body());

        assertEquals("[{\"name\":\"alpha\",\"count\":\"5\",\"note\":\"first\"},"
            + "{\"name\":\"b\",\"count\":\"120\",\"note\":null},"
            + "{\"name\":\"gamma\",\"count\":\"7\",\"note\":\"third\"}]", json.toString());
    }

    @Test
    void jsonKeysAreTheTokensAsTyped() throws Exception {
        JsonNode json = MAPPER.readTree(CatRendering.render(request("format", "json", "h", "n,c"), table()).body());

        List<String> keys = new java.util.ArrayList<>();
        json.get(0).fieldNames().forEachRemaining(keys::add);
        assertEquals(List.of("n", "c"), keys, "n and c, not name and count");
    }

    @Test
    void anEmptyTableIsAnEmptyJsonArray() {
        assertEquals("[]", new String(CatRendering.render(request("format", "json"), new CatTable(COLUMNS)).body(), StandardCharsets.UTF_8));
    }

    @Test
    void anAcceptHeaderOfApplicationJsonAsksForJson() {
        RestRequest asJson = request(Map.of("accept", "application/json"));
        assertTrue(text(CatRendering.render(asJson, table())).startsWith("[{"));
        RestRequest asAnything = request(Map.of("accept", "*/*"));
        assertTrue(text(CatRendering.render(asAnything, table())).startsWith("alpha"));
    }

    @Test
    void formatBeatsAcceptAndAnUnknownFormatIsText() {
        RestRequest request = request(Map.of("accept", "application/json"), "format", "txt");
        assertTrue(text(CatRendering.render(request, table())).startsWith("alpha"));
    }

    @Test
    void formatsThatCannotBeProducedAreRefused() {
        for (String format : List.of("yaml", "smile", "cbor")) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CatRendering.render(request("format", format), table()));
            assertEquals("format [" + format + "] is not supported; use json or text", e.getMessage());
        }
    }

    // ------------------------------------------------------------------ times

    @Test
    void aTimeReadsAsATimeValueOrInTheUnitAskedFor() {
        CatTable table = new CatTable(List.of(Column.right("wait", "", "")));
        table.row(Duration.ofMillis(1500));

        assertEquals("1.5s\n", render(table));
        assertEquals("1500\n", render(table, "time", "ms"));
        assertEquals("1\n", render(table, "time", "s"));
        assertEquals("1.5s\n", render(table, "time", "bogus"), "an unrecognised unit is ignored, as upstream ignores it");
    }

    @Test
    void timeValueFormattingTruncatesToOneDecimalAndDropsAZero() {
        assertEquals("0s", TimeValues.format(Duration.ZERO));
        assertEquals("15ms", TimeValues.format(Duration.ofMillis(15)));
        assertEquals("3.2s", TimeValues.format(Duration.ofMillis(3200)));
        assertEquals("1s", TimeValues.format(Duration.ofMillis(1050)), "1.05 truncates to 1.0, which drops its zero");
        assertEquals("1.9s", TimeValues.format(Duration.ofMillis(1999)), "truncated, not rounded");
        assertEquals("1.5m", TimeValues.format(Duration.ofSeconds(90)));
        assertEquals("2h", TimeValues.format(Duration.ofHours(2)));
        assertEquals("3d", TimeValues.format(Duration.ofDays(3)));
        assertEquals("250micros", TimeValues.format(Duration.ofNanos(250_000)));
        assertEquals("999nanos", TimeValues.format(Duration.ofNanos(999)));
    }
}
