package org.opensearch.supervisor.model.index;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.status.Replication;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * What the supervisor needs from {@code tenant/<t>/indices/<name>} — the control plane's index document,
 * and the only statement of desired state that matters for placement.
 *
 * <p>This is a <b>projection of that document, not a model of it</b>. The control plane owns the full
 * shape; reproducing it here would create a second definition of the same thing that drifts. Everything not
 * needed to decide where shards go is deliberately dropped.
 *
 * <p>Three properties of the source document, from {@code KEY_LAYOUT.md}, that this class exists to absorb:
 *
 * <ul>
 *   <li><b>{@code settings} is flat and every value is a string.</b> {@code "3"}, not {@code 3};
 *       {@code "true"}, not {@code true}. Keys are always fully qualified with the {@code index.} prefix
 *       whatever shape the client sent, because the nested form is a rendering applied on the way out.
 *   <li><b>{@code state} is {@code open} or {@code close}.</b> A closed index still has settings and
 *       mappings and still exists; it simply should not be serving.
 *   <li><b>{@code uuid} is the identity, not the name.</b> Names are reused.
 * </ul>
 */
public record IndexConfig(
    String name,
    String uuid,
    boolean open,
    int numberOfShards,
    int numberOfReplicas,
    Replication replication,
    boolean hidden,
    long configRevision
) {

    public static final String SETTING_SHARDS = "index.number_of_shards";
    public static final String SETTING_REPLICAS = "index.number_of_replicas";
    public static final String SETTING_REPLICATION = "index.replication.type";
    public static final String SETTING_HIDDEN = "index.hidden";

    public IndexConfig {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uuid, "uuid");
        if (numberOfShards < 1) {
            throw new IllegalArgumentException("index [" + name + "] declares " + numberOfShards + " shards");
        }
        if (numberOfReplicas < 0) {
            throw new IllegalArgumentException("index [" + name + "] declares " + numberOfReplicas + " replicas");
        }
    }

    /** Copies desired per shard: one primary plus its replicas. */
    public int copiesPerShard() {
        return 1 + numberOfReplicas;
    }

    /** Total copies this index wants across the pool. Zero while closed — a closed index serves nothing. */
    public int desiredCopies() {
        return open ? numberOfShards * copiesPerShard() : 0;
    }

    /**
     * Parses the control plane's document.
     *
     * @param configRevision the store revision the bytes were read at, which the assignment passes to the
     *     node so it can tell whether it is building against the configuration the supervisor intended
     * @throws Json.MalformedDocumentException if the document cannot be read as an index at all. A missing
     *     or unparseable <em>setting</em> falls back to a default instead: one bad number should cost the
     *     index a sensible shard count, not cost the supervisor the whole index.
     */
    public static IndexConfig parse(byte[] bytes, long configRevision) {
        JsonNode root = Json.read(bytes, JsonNode.class, "index document");

        String name = text(root, "index", null);
        String uuid = text(root, "uuid", null);
        if (name == null || uuid == null) {
            throw new IllegalArgumentException(
                "index document has no " + (name == null ? "index" : "uuid") + " field; refusing to guess"
            );
        }

        JsonNode settings = root.path("settings");
        return new IndexConfig(
            name,
            uuid,
            "close".equalsIgnoreCase(text(root, "state", "open")) == false,
            intSetting(settings, SETTING_SHARDS, 1, name),
            intSetting(settings, SETTING_REPLICAS, 0, name),
            Replication.from(stringSetting(settings, SETTING_REPLICATION, "SEGMENT")),
            Boolean.parseBoolean(stringSetting(settings, SETTING_HIDDEN, "false")),
            configRevision
        );
    }

    private static String text(JsonNode node, String field, String fallback) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? fallback : value.asText();
    }

    /** Settings are strings, always — {@code asInt} on a {@code "3"} would silently answer 0. */
    private static String stringSetting(JsonNode settings, String key, String fallback) {
        JsonNode value = settings.get(key);
        return value == null || value.isNull() ? fallback : value.asText();
    }

    private static int intSetting(JsonNode settings, String key, int fallback, String index) {
        String raw = stringSetting(settings, key, null);
        if (raw == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            // Deliberately not fatal. A tenant that has somehow written a nonsense shard count should get a
            // finding about that index, not a supervisor that cannot observe any of its indices.
            throw new IllegalArgumentException(
                "index [" + index + "] setting [" + key + "] is not a number: [" + raw + "]", e
            );
        }
    }

    /** For tests and for the fake data plane; the control plane writes the real thing. */
    public static byte[] toDocument(
        String name,
        String uuid,
        boolean open,
        Map<String, String> settings
    ) {
        StringBuilder json = new StringBuilder("{\"index\":\"").append(name)
            .append("\",\"uuid\":\"").append(uuid)
            .append("\",\"state\":\"").append(open ? "open" : "close")
            .append("\",\"settings\":{");
        boolean first = true;
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            if (first == false) {
                json.append(',');
            }
            first = false;
            json.append('"').append(entry.getKey()).append("\":\"").append(entry.getValue()).append('"');
        }
        return json.append("}}").toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    public static Map<String, String> settingsOf(int shards, int replicas, Replication replication) {
        return Map.of(
            SETTING_SHARDS, Integer.toString(shards),
            SETTING_REPLICAS, Integer.toString(replicas),
            SETTING_REPLICATION, replication.name().toUpperCase(Locale.ROOT)
        );
    }
}
