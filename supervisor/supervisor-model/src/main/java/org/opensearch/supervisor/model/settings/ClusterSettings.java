package org.opensearch.supervisor.model.settings;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.supervisor.model.Json;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * What the supervisor needs from {@code tenant/<t>/cluster/settings}, which the control plane writes as
 * {@code {"persistent": {…}, "transient": {…}}} with flat, fully-qualified keys.
 *
 * <p>A projection of that document rather than a model of it, like {@code IndexConfig}: the control plane owns
 * the shape and reproducing it here would create a second definition that drifts.
 *
 * <p><b>Transient wins over persistent</b>, matching OpenSearch. That ordering is the whole point of the
 * distinction — transient is what an operator sets for the duration of an incident, and it would be useless if
 * a persistent value could override it.
 */
public record ClusterSettings(Map<String, String> persistent, Map<String, String> transientSettings) {

    public static final String ALLOCATION_ENABLE = "cluster.routing.allocation.enable";
    public static final String REBALANCE_ENABLE = "cluster.routing.rebalance.enable";

    public static final ClusterSettings EMPTY = new ClusterSettings(Map.of(), Map.of());

    public ClusterSettings {
        persistent = persistent == null ? Map.of() : Map.copyOf(persistent);
        transientSettings = transientSettings == null ? Map.of() : Map.copyOf(transientSettings);
    }

    public Optional<String> get(String key) {
        String value = transientSettings.get(key);
        return Optional.ofNullable(value != null ? value : persistent.get(key));
    }

    /** {@link AllocationEnable#ALL} when unset, which is OpenSearch's default and the only safe absence. */
    public AllocationEnable allocationEnable() {
        return get(ALLOCATION_ENABLE).map(AllocationEnable::from).orElse(AllocationEnable.ALL);
    }

    /** Whether the value present is one this build understands, so an operator's typo becomes visible. */
    /**
     * The rebalance brake, defaulting to {@link RebalanceEnable#ALL} when unset.
     *
     * <p>Unset defaults to on, while an unrecognised value refuses — the asymmetry is the point. Silence means
     * an operator never expressed an opinion and should get the sensible default; a value they typed means they
     * did, and guessing at it would be ignoring them.
     */
    public RebalanceEnable rebalanceEnable() {
        return get(REBALANCE_ENABLE).map(RebalanceEnable::from).orElse(RebalanceEnable.ALL);
    }

    /** Whether the rebalance setting, if present at all, was a value this understands. */
    public boolean rebalanceEnableIsRecognised() {
        return get(REBALANCE_ENABLE).map(value -> RebalanceEnable.from(value) != RebalanceEnable.UNKNOWN)
            .orElse(true);
    }

    public boolean allocationEnableIsRecognised() {
        return get(ALLOCATION_ENABLE).map(value -> AllocationEnable.from(value) != AllocationEnable.UNKNOWN)
            .orElse(true);
    }

    public static ClusterSettings parse(byte[] bytes) {
        JsonNode root = Json.read(bytes, JsonNode.class, "cluster settings");
        return new ClusterSettings(flatten(root.get("persistent")), flatten(root.get("transient")));
    }

    /**
     * Values are read as text whatever they look like in JSON.
     *
     * <p>The control plane normalises index settings to strings but {@code cluster/settings} carries whatever a
     * client sent, so a boolean or a number can appear here where a string appears there. {@code asText} on a
     * JSON boolean gives {@code "true"}, which is what a setting parser wants; {@code asText} on a string node
     * is the string. Reading it as anything narrower would drop half the values a client can legally send.
     */
    private static Map<String, String> flatten(JsonNode node) {
        Map<String, String> flat = new LinkedHashMap<>();
        if (node == null || node.isObject() == false) {
            return flat;
        }
        node.properties().forEach(field -> {
            if (field.getValue().isValueNode()) {
                flat.put(field.getKey(), field.getValue().asText());
            }
        });
        return flat;
    }
}
