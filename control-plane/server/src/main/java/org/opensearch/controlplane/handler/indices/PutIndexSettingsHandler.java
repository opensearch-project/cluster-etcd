/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.capability.SettingsRegistry;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.metadata.SettingsValidation;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * {@code PUT /{index}/_settings} — changes settings on existing indices.
 *
 * <p>The first operation that validates settings, and therefore the first that needs the data plane's
 * settings registry. Without one — the default — nothing is checked and anything is accepted, exactly
 * as before. See {@link SettingsRegistry}.
 *
 * <h2>What gets rejected, and how</h2>
 *
 * <p>Three outcomes, all verified against OpenSearch 3.9.0:
 *
 * <ul>
 *   <li><b>Private setting</b> — {@code 400 settings_exception},
 *       {@code can not update private setting [index.uuid]; this setting is managed by OpenSearch}.
 *       Distinct from both the unknown-setting message and from what {@code indices.create} says for
 *       the same condition.</li>
 *   <li><b>Unknown setting</b> — {@code 400 settings_exception},
 *       {@code unknown setting [index.totally_bogus] please check that any required plugins are
 *       installed...}, or {@code ... did you mean [index.number_of_shards]?} when a registered key
 *       scores close enough.</li>
 *   <li><b>Non-dynamic setting while any target index is open</b> —
 *       {@code 400 illegal_argument_exception}, {@code Can't update non dynamic settings
 *       [[index.number_of_shards]] for open indices [[my-index/uuid]]}. Both lists render with their own
 *       brackets, hence the doubling.</li>
 *   <li><b>Final setting once every target is closed</b> — {@code 400 settings_exception},
 *       {@code final my-index setting [index.number_of_shards], not updateable}.</li>
 *   <li><b>Anything else</b> — {@code {"acknowledged": true}}.</li>
 * </ul>
 *
 * <h2>State decides which rejection applies</h2>
 *
 * <p>The distinction that makes {@code indices.close} worth having. A <b>static</b> setting — neither
 * {@code Dynamic} nor {@code Final}, which is 45 of the 221 index settings — may be changed on a
 * <em>closed</em> index and not on an open one. A {@code Final} setting may not be changed even then.
 *
 * <p>And the two report differently depending on state, which took a probe to establish. While any
 * target index is open, a real cluster pre-filters everything non-dynamic before reaching the code that
 * tells static from final, so both answer the "for open indices" message. Once every target is closed,
 * static settings are simply applied and only final ones are refused — with their own wording. Verified:
 * {@code index.codec} is accepted on a closed index and refused on an open one, while
 * {@code index.number_of_shards} is refused on both, differently.
 *
 * <h2>Two behaviours that look like details and are not</h2>
 *
 * <p><b>A null removes the setting.</b> Verified: {@code {"index":{"refresh_interval":null}}} leaves the
 * index with no {@code refresh_interval} at all, so it reverts to its default rather than being stored
 * as null. Null keys are excluded from validation for the same reason — removing a setting is not
 * setting it.
 *
 * <p><b>{@code preserve_existing=true} skips keys the index already has</b>, rather than failing on
 * them. Verified in both directions: a key already present keeps its old value, and an absent one is
 * set.
 *
 * <h2>Atomicity and concurrency</h2>
 *
 * <p>As with {@code put_mapping}: validate against every resolved index first and write only once all
 * of them pass, so a rejection on the last leaves the first untouched. Each write is a compare-and-set
 * on the revision the metadata was read at, and losing that race is a 409.
 */
public final class PutIndexSettingsHandler implements OperationHandler {

    private final SettingsRegistry registry;

    public PutIndexSettingsHandler(SettingsRegistry registry) {
        this.registry = registry;
    }

    @Override
    public String operationGroup() {
        return "indices.put_settings";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        if (request.hasBody() == false) {
            return ErrorResponses.parseFailure(request, "request body is required");
        }
        JsonNode body;
        try {
            body = Json.parse(request.body());
        } catch (IOException e) {
            return ErrorResponses.parseFailure(request, "failed to parse settings: " + e.getMessage());
        }
        if (body == null || body.isObject() == false) {
            return ErrorResponses.parseFailure(request, "settings must be a JSON object");
        }

        // The body is a settings document in its own right -- {"index":{...}} or the flat and
        // unprefixed forms -- so the same normalisation applies as everywhere else.
        Settings requested = Settings.forIndex(body);
        Set<String> removals = requested.nullKeys();
        Settings assignments = requested.withoutNulls();

        // Removing a setting is not setting it, so nulls are not checked against the registry.
        SettingsValidation.Problems problems =
            SettingsValidation.check(assignments, registry, SettingsRegistry.Scope.INDEX, true);
        if (problems.unknown().isEmpty() == false) {
            String key = problems.unknown().getFirst();
            return ErrorResponses.unknownSetting(request, key,
                SettingsValidation.didYouMean(key, registry.exactKeys(SettingsRegistry.Scope.INDEX)).orElse(null));
        }
        if (problems.notUserSettable().isEmpty() == false) {
            return ErrorResponses.privateSettingOnUpdate(request, problems.notUserSettable().getFirst());
        }

        IndexExpressions.Resolved resolved =
            IndexExpressions.resolve(request, context.store(), IndicesOptions.strictExpandOpen());
        if (resolved.failed()) {
            return resolved.failure();
        }

        IndexStore indices = new IndexStore(context.store());
        boolean preserveExisting = request.paramAsBoolean("preserve_existing", false);

        List<Pending> pending = new ArrayList<>();
        List<String> openIndices = new ArrayList<>();
        for (String index : resolved.indices().stream().sorted().toList()) {
            Optional<KeyValueStore.Versioned> stored = indices.getVersioned(index);
            if (stored.isEmpty()) {
                return ErrorResponses.indexNotFound(request, index);
            }
            ObjectNode metadata = (ObjectNode) parse(stored.get());
            if (IndexStore.isOpen(metadata)) {
                // Rendered the way OpenSearch's Index.toString does, brackets included; see
                // ErrorResponses.nonDynamicSettings for why that matters to the message.
                openIndices.add("[" + index + "/" + metadata.path("uuid").asText("_na_") + "]");
            }
            pending.add(new Pending(index, metadata, stored.get().revision()));
        }

        // Which rejection applies depends on the indices' state, and the split is not cosmetic.
        //
        // While any target index is open, a real cluster pre-filters everything non-dynamic before it
        // reaches the code that distinguishes static from final, so both report the same "for open
        // indices" message. Once every target is closed, static settings become legal and only final
        // ones are refused -- with their own wording, naming the index rather than listing it.
        if (openIndices.isEmpty() == false) {
            List<String> refused = problems.allNotDynamic();
            if (refused.isEmpty() == false) {
                return ErrorResponses.nonDynamicSettings(request, refused, openIndices);
            }
        } else if (problems.notEverUpdateable().isEmpty() == false) {
            return ErrorResponses.finalSetting(
                request, pending.getFirst().index(), problems.notEverUpdateable().getFirst());
        }

        IndexManagementTasks tasks = new IndexManagementTasks(context.store());
        for (Pending each : pending) {
            Settings current = Settings.forIndex(each.metadata().path("settings"));
            Settings updates = preserveExisting ? withoutKeysAlreadySet(assignments, current) : assignments;
            Settings merged = current.mergedWith(updates).without(removals);

            each.metadata().set("settings", merged.toJson(true));
            if (indices.replace(each.index(), each.metadata(), each.revision()) == false) {
                return ErrorResponses.concurrentUpdate(request, each.index());
            }
            tasks.submit(
                IndexManagementTasks.TYPE_UPDATE_SETTINGS,
                each.index(),
                each.metadata().path("uuid").asText("_na_"),
                context.nowEpochMillis()
            );
        }
        return Json.acknowledged(request);
    }

    /** {@code preserve_existing}: a key the index already carries is left alone, not rejected. */
    private static Settings withoutKeysAlreadySet(Settings updates, Settings current) {
        Set<String> alreadySet = new java.util.LinkedHashSet<>(updates.keys());
        alreadySet.removeIf(key -> current.contains(key) == false);
        return updates.without(alreadySet);
    }

    /** An index whose merged metadata is ready to write, and the revision it must still be at. */
    private record Pending(String index, ObjectNode metadata, long revision) {}

    private static JsonNode parse(KeyValueStore.Versioned versioned) {
        try {
            return Json.parse(versioned.value());
        } catch (IOException e) {
            throw new IllegalStateException("corrupt index metadata", e);
        }
    }
}
