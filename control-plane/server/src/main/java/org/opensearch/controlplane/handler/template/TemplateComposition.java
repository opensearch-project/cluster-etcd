/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.NameExpression;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Composable index template resolution: which template applies to an index name, and what it composes to.
 *
 * <p>The rules were established by probing {@code _simulate_index} against OpenSearch 3.9.0 rather than
 * by reading {@code MetadataIndexTemplateService}, because the composed output is directly observable and
 * the source is large. What was confirmed:
 *
 * <ul>
 *   <li><b>Component templates merge in {@code composed_of} order, later winning.</b> With
 *       {@code [c1, c2]} where c1 sets {@code number_of_replicas: 9} and c2 sets {@code 5}, the result is
 *       5; reversing the list gives 9.</li>
 *   <li><b>The index template's own {@code template} block beats every component.</b> Setting
 *       {@code number_of_replicas: 1} there wins over both.</li>
 *   <li>Settings override key by key; mappings and aliases merge structurally, so
 *       {@code properties.a} from one component and {@code properties.b} from another both survive.</li>
 *   <li>Among several matching templates the highest {@code priority} wins outright — templates are
 *       selected, not merged with each other.</li>
 * </ul>
 *
 * <p>"Settings override key by key" is the reason settings are accumulated as a {@link Settings} rather
 * than merged structurally like the other two blocks. On the flat canonical form an override is a map
 * {@code put}, so it lands whichever of the accepted shapes each side used. Structurally it did not: a
 * component saying {@code {"index": {"number_of_replicas": 9}}} beneath a template block saying
 * {@code {"number_of_replicas": 5}} produced <em>both</em> keys, and {@code _simulate_index} showed them
 * both where a real cluster shows one.
 *
 * <h2>Legacy templates are an alternative, not a layer</h2>
 *
 * <p>Legacy {@code _template} definitions apply <b>only when no composable template matches</b>. When
 * one does, the legacy ones contribute nothing whatever. Verified: adding a matching composable
 * template made a created index fall back to the built-in {@code number_of_replicas: 1} rather than the
 * {@code 5} a matching legacy template specified, and dropped its mappings and aliases with it.
 *
 * <p>Where a composable template is selected, legacy templates are <b>merged</b> — all of them, in
 * ascending {@code order}, later winning. That is what {@code order} is for, and it is why overlapping
 * patterns are rejected for composable templates and legal for legacy ones. Verified with two templates
 * at orders 0 and 10: the higher one's {@code number_of_replicas} won while the lower one's
 * {@code refresh_interval} survived, and their mappings and aliases both accumulated.
 *
 * <h2>Not implemented</h2>
 *
 * <p><b>Mapping conflict detection.</b> Merging is structural, so two templates disagreeing about a
 * field's type produce a last-writer-wins mapping rather than the rejection a real cluster would raise.
 * That belongs with {@code put_mapping}'s merge rules.
 */
public final class TemplateComposition {

    private final TemplateStore indexTemplates;
    private final TemplateStore componentTemplates;
    private final TemplateStore legacyTemplates;

    TemplateComposition(KeyValueStore store) {
        this.indexTemplates = new TemplateStore(store, TemplateKind.index());
        this.componentTemplates = new TemplateStore(store, TemplateKind.component());
        this.legacyTemplates = new TemplateStore(store, TemplateKind.legacy());
    }

    /** A matching template and its name, so callers can report which one applied. */
    public record Match(String name, ObjectNode template) {}

    /**
     * The highest-priority composable template whose patterns match, {@code findV2Template}'s job.
     *
     * <p>Public for the data stream handlers, which need the <em>template</em> and not just what it
     * composes to: creating a data stream reads the template's {@code data_stream} block for the timestamp
     * field, and reports the template's name both in its errors and in {@code get_data_stream}. Every
     * other caller wants {@link #composeFor}.
     */
    public static Optional<Match> matchingIndexTemplate(KeyValueStore store, String indexName) {
        return new TemplateComposition(store).findMatching(indexName);
    }

    /**
     * The highest-priority composable template whose patterns match, if any.
     *
     * <p>Templates are selected rather than merged: a lower-priority match contributes nothing.
     */
    Optional<Match> findMatching(String indexName) {
        Match best = null;
        int bestPriority = Integer.MIN_VALUE;

        for (Map.Entry<String, ObjectNode> entry : indexTemplates.all().entrySet()) {
            if (matchesAnyPattern(entry.getValue(), indexName) == false) {
                continue;
            }
            int priority = entry.getValue().path("priority").asInt(0);
            if (priority > bestPriority) {
                bestPriority = priority;
                best = new Match(entry.getKey(), entry.getValue());
            }
        }
        return Optional.ofNullable(best);
    }

    /**
     * Composes a template to the {@code settings}/{@code mappings}/{@code aliases} an index would get.
     */
    Composed compose(ObjectNode template) {
        Accumulator accumulator = new Accumulator();
        composeInto(accumulator, template);
        return accumulator.composed();
    }

    /** Components first, in {@code composed_of} order; then the template's own block, which beats them. */
    private void composeInto(Accumulator accumulator, ObjectNode template) {
        for (JsonNode reference : template.path("composed_of")) {
            componentTemplates.get(reference.asText())
                .ifPresent(component -> accumulator.merge(component.path("template")));
        }
        accumulator.merge(template.path("template"));
    }

    /**
     * Every matching legacy template, in the order they should be applied.
     *
     * <p>Ascending {@code order}, so a higher one is applied later and wins. Unlike composable
     * templates these are <b>merged rather than selected</b> — that is what {@code order} is for, and it
     * is why overlapping patterns are legal here and rejected there.
     *
     * <p>Ties are broken by name, ascending. Verified: two templates at {@code order: 10} resolved to
     * the alphabetically later one's value. OpenSearch sorts on {@code order} alone, so strictly the tie
     * is however its map happened to iterate; sorting by name makes ours deterministic and matches what
     * the live cluster did.
     */
    private List<ObjectNode> matchingLegacyTemplates(String indexName) {
        return legacyTemplates.all().entrySet().stream()
            .filter(entry -> matchesAnyPattern(entry.getValue(), indexName))
            .sorted(Comparator
                .<Map.Entry<String, ObjectNode>>comparingInt(entry -> entry.getValue().path("order").asInt(0))
                .thenComparing(Map.Entry::getKey))
            .map(Map.Entry::getValue)
            .toList();
    }

    /** Templates whose patterns overlap the given ones at the same priority. Used to validate a put. */
    List<String> conflictingTemplates(String name, ObjectNode candidate) {
        List<String> patterns = patternsOf(candidate);
        int priority = candidate.path("priority").asInt(0);

        List<String> conflicts = new ArrayList<>();
        indexTemplates.all().forEach((existingName, existing) -> {
            if (existingName.equals(name)) {
                return;
            }
            if (existing.path("priority").asInt(0) != priority) {
                // Overlap is fine at different priorities; the higher one simply wins.
                return;
            }
            if (patternsOverlap(patterns, patternsOf(existing))) {
                conflicts.add(existingName);
            }
        });
        return conflicts;
    }

    static List<String> patternsOf(ObjectNode template) {
        List<String> patterns = new ArrayList<>();
        template.path("index_patterns").forEach(pattern -> patterns.add(pattern.asText()));
        return patterns;
    }

    private static boolean matchesAnyPattern(ObjectNode template, String indexName) {
        return patternsOf(template).stream()
            .anyMatch(pattern -> NameExpression.toPattern(pattern).matcher(indexName).matches());
    }

    /**
     * Whether two pattern sets could ever match the same name.
     *
     * <p>Approximated: exact equality, or either side matching the other as a glob. Full pattern
     * intersection is undecidable in general and OpenSearch does not attempt it either, but this is
     * looser than the server's check, so a genuinely ambiguous pair could slip through.
     */
    private static boolean patternsOverlap(List<String> left, List<String> right) {
        for (String a : left) {
            for (String b : right) {
                if (a.equals(b)
                    || NameExpression.toPattern(a).matcher(b).matches()
                    || NameExpression.toPattern(b).matcher(a).matches()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * The composed configuration for a new index: whatever template applies, overridden by the request.
     *
     * <p>Public because {@code CreateIndexHandler} needs it; the rest of the class is package-private.
     *
     * <p>Composable and legacy templates are <b>alternatives, not layers</b>. If any composable template
     * matches, the legacy ones contribute nothing at all — verified against 3.9.0, where adding a
     * matching composable template made an index fall back to the built-in
     * {@code number_of_replicas: 1} rather than the {@code 5} a matching legacy template specified, and
     * dropped its mappings and aliases too. Only when no composable template matches do the legacy ones
     * apply, and then all of them merge.
     *
     * @param requested the create request body, which wins over anything a template supplied
     */
    public static Composed composeFor(KeyValueStore store, String indexName, ObjectNode requested) {
        TemplateComposition composition = new TemplateComposition(store);
        Accumulator accumulator = new Accumulator();

        Optional<Match> composable = composition.findMatching(indexName);
        if (composable.isPresent()) {
            composition.composeInto(accumulator, composable.get().template());
        } else {
            // Legacy definitions carry settings/mappings/aliases at their root rather than under a
            // template block, so the definition itself is the block to merge.
            composition.matchingLegacyTemplates(indexName).forEach(accumulator::merge);
        }

        // The request body carries the same three blocks and is merged like one more, last, so it wins.
        accumulator.merge(requested);
        return accumulator.composed();
    }

    /**
     * What a template composes to.
     *
     * <p>Settings stay a {@link Settings} rather than JSON because the two callers want different
     * renderings of them: {@code indices.create} stores the flat canonical form, while
     * {@code _simulate_index} answers the nested one.
     */
    public record Composed(Settings settings, ObjectNode mappings, ObjectNode aliases) {

        /** The wire shape both simulate operations return: three keys, each possibly empty. */
        ObjectNode toJson() {
            ObjectNode json = Json.object();
            json.set("settings", settings.toJson(false));
            json.set("mappings", mappings);
            json.set("aliases", aliases);
            return json;
        }
    }

    /** Accumulates blocks in precedence order: settings flat and key-by-key, the rest structurally. */
    private static final class Accumulator {

        private Settings settings = Settings.EMPTY;
        private final ObjectNode mappings = Json.object();
        private final ObjectNode aliases = Json.object();

        void merge(JsonNode block) {
            if (block.isObject() == false) {
                return;
            }
            settings = settings.mergedWith(Settings.forIndex(block.path("settings")));
            mergeInto(mappings, block.path("mappings"));
            mergeInto(aliases, block.path("aliases"));
        }

        Composed composed() {
            return new Composed(settings, mappings, aliases);
        }
    }

    /** Structural merge, later winning. Objects recurse; anything else replaces. */
    static void mergeInto(ObjectNode target, JsonNode source) {
        if (source.isObject() == false) {
            return;
        }
        source.properties().forEach(field -> {
            JsonNode existing = target.get(field.getKey());
            if (existing != null && existing.isObject() && field.getValue().isObject()) {
                mergeInto((ObjectNode) existing, field.getValue());
            } else {
                target.set(field.getKey(), field.getValue().deepCopy());
            }
        });
    }

}
