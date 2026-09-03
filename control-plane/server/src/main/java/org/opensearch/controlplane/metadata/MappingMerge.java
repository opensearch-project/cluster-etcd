/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;

/**
 * Merging a mapping update into an existing mapping, rejecting what a real cluster would reject.
 *
 * <p>Additive changes are free: a new field, a new property under an existing object, a new
 * multi-field under {@code fields}. Changes to a field that already exists are checked against
 * {@link MappingParameters}, because a parameter that decided how bytes were written cannot be changed
 * without invalidating what is already on disk.
 *
 * <p>Two failures, reported differently — both wordings verified against OpenSearch 3.9.0:
 *
 * <table border="1">
 *   <caption>Conflict messages</caption>
 *   <tr><th>change</th><th>message</th></tr>
 *   <tr><td>type</td><td>{@code mapper [t] cannot be changed from type [text] to [long]}</td></tr>
 *   <tr><td>parameter</td><td>{@code Mapper for [t] conflicts with existing mapper:\n\tCannot update
 *       parameter [index] from [true] to [false]}</td></tr>
 * </table>
 *
 * <p>Note the second is a two-line message with a literal newline and tab, and that both name the field
 * by its <b>full dotted path</b> — a conflict on {@code user.name} reports {@code mapper [user.name]}.
 *
 * <h2>What this is not</h2>
 *
 * <p>Not {@code MapperService.merge}. Mappings are still stored as sent rather than parsed into field
 * mappers, so this compares JSON rather than models. That is enough to decide updateability, which is a
 * per-parameter fact, but it means a mapping that is malformed in some way unrelated to merging — an
 * unknown field type, a parameter of the wrong shape — is accepted here and would be rejected by a real
 * cluster at parse time. Same boundary the rest of the service draws: structure yes, mapping engine no.
 *
 * <h2>The one place this is stricter than a real cluster</h2>
 *
 * <p>A real cluster compares an update against the mapper's <b>resolved</b> value, defaults included.
 * Given a field stored as {@code {"type": "text"}} it knows {@code index} is {@code true}, so
 * {@code index: false} is a change and {@code index: true} is a no-op — verified, and it accepts the
 * second. This service has no defaults to resolve against, because knowing them per field type is the
 * mapping engine.
 *
 * <p>So <b>declaring an immutable parameter that the stored mapping does not declare is treated as a
 * change</b>, whatever its value. That rejects one thing a real cluster accepts: restating an immutable
 * parameter at exactly its default. It never does the opposite, which is the point — accepting a change
 * to how data is indexed would leave a mapping the data plane cannot honour, where a false rejection is
 * visible immediately and worked around by sending only the fields being added.
 *
 * <p>The message drops the {@code from [x]} clause in that case rather than inventing a previous value
 * it does not know.
 */
public final class MappingMerge {

    /** Why an update was rejected: the message a real cluster would have produced. */
    public record Conflict(String message) {}

    private MappingMerge() {}

    /**
     * Merges {@code update} into {@code existing}, in place, unless it conflicts.
     *
     * <p>Checked before anything is written: on a conflict {@code existing} is left untouched, so a
     * caller can validate several indices before committing any of them.
     *
     * @param existing the stored mappings document; mutated only when the merge succeeds
     * @param update   the mapping update, the same shape as a mappings document
     * @return the conflict, or empty when the merge was applied
     */
    public static Optional<Conflict> merge(ObjectNode existing, JsonNode update) {
        Optional<Conflict> conflict = checkProperties("", existing, update);
        if (conflict.isPresent()) {
            return conflict;
        }
        apply(existing, update);
        return Optional.empty();
    }

    /** Walks both trees together looking for a field that exists on each and disagrees. */
    private static Optional<Conflict> checkProperties(String path, JsonNode existing, JsonNode update) {
        if (update.isObject() == false || existing.isObject() == false) {
            return Optional.empty();
        }
        for (String container : new String[] { "properties", "fields" }) {
            JsonNode updatedFields = update.get(container);
            JsonNode existingFields = existing.get(container);
            if (updatedFields == null || updatedFields.isObject() == false
                || existingFields == null || existingFields.isObject() == false) {
                continue;
            }
            for (var field : updatedFields.properties()) {
                JsonNode before = existingFields.get(field.getKey());
                if (before == null) {
                    // A new field. Nothing to conflict with.
                    continue;
                }
                String fieldPath = path.isEmpty() ? field.getKey() : path + "." + field.getKey();
                Optional<Conflict> conflict = checkField(fieldPath, before, field.getValue());
                if (conflict.isPresent()) {
                    return conflict;
                }
            }
        }
        return Optional.empty();
    }

    /** One field that exists on both sides: its type, then each parameter, then its children. */
    private static Optional<Conflict> checkField(String path, JsonNode before, JsonNode after) {
        if (after.isObject() == false || before.isObject() == false) {
            return Optional.empty();
        }
        String existingType = before.path(MappingParameters.TYPE).asText(null);
        String updatedType = after.path(MappingParameters.TYPE).asText(null);
        if (updatedType != null && existingType != null && updatedType.equals(existingType) == false) {
            return Optional.of(new Conflict(
                "mapper [" + path + "] cannot be changed from type [" + existingType + "] to [" + updatedType + "]"
            ));
        }

        for (var parameter : after.properties()) {
            String name = parameter.getKey();
            if (name.equals("properties") || name.equals("fields")) {
                // Containers recurse; they are additive and checked field by field below.
                continue;
            }
            JsonNode previous = before.get(name);
            if (previous != null && previous.equals(parameter.getValue())) {
                continue;
            }
            if (MappingParameters.isUpdateable(name, existingType)) {
                continue;
            }
            // The server would say "from [x] to [y]" using the mapper's resolved value. Where the
            // stored mapping never declared the parameter there is no honest x to name, so the clause
            // is dropped rather than invented. See the class javadoc.
            String change = previous == null
                ? "Cannot update parameter [" + name + "] to [" + parameter.getValue().asText() + "]"
                : "Cannot update parameter [" + name + "] from [" + previous.asText() + "] to ["
                    + parameter.getValue().asText() + "]";
            return Optional.of(new Conflict(
                "Mapper for [" + path + "] conflicts with existing mapper:\n\t" + change
            ));
        }
        return checkProperties(path, before, after);
    }

    /** Structural merge, the update winning. Only reached once the whole update has been checked. */
    private static void apply(ObjectNode target, JsonNode source) {
        if (source.isObject() == false) {
            return;
        }
        source.properties().forEach(field -> {
            JsonNode existing = target.get(field.getKey());
            if (existing != null && existing.isObject() && field.getValue().isObject()) {
                apply((ObjectNode) existing, field.getValue());
            } else {
                target.set(field.getKey(), field.getValue().deepCopy());
            }
        });
    }
}
