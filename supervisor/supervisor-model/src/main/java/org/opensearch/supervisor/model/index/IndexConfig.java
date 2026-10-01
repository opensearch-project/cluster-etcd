package org.opensearch.supervisor.model.index;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.settings.AllocationEnable;
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
    int numberOfSearchReplicas,
    Replication replication,
    boolean hidden,
    boolean ingestionPaused,
    AllocationEnable allocationEnable,
    long configRevision,
    Map<Integer, Integer> copyOverrides
) {

    public static final String SETTING_SHARDS = "index.number_of_shards";
    public static final String SETTING_REPLICAS = "index.number_of_replicas";

    /**
     * Read-only copies that recover from the remote store and are never promoted.
     *
     * <p>Independent of {@link #SETTING_REPLICAS} in OpenSearch, and it was inert here until a tenant wrote it
     * and got a green index with no copy to show for it.
     */
    public static final String SETTING_SEARCH_REPLICAS = "index.number_of_search_replicas";
    public static final String SETTING_REPLICATION = "index.replication.type";
    public static final String SETTING_HIDDEN = "index.hidden";

    /**
     * Whether the index's pull-based consumer is stopped.
     *
     * <p><b>Not a setting</b>, which is the trap: it is a first-class field on OpenSearch's {@code IndexMetadata}
     * as {@code IngestionStatus.isPaused}, so the obvious guess of {@code index.ingestion.paused} in the settings
     * block finds nothing. The control plane writes it at the top level of the index document, matching upstream's
     * spelling exactly.
     */
    public static final String FIELD_INGESTION_STATUS = "ingestion_status";

    public static final String FIELD_IS_PAUSED = "is_paused";

    /** Overrides the cluster-wide value, so one index can be frozen without freezing the rest. */
    public static final String SETTING_ALLOCATION_ENABLE = "index.routing.allocation.enable";

    /** Everything except the ingestion pause, which only the ingestion tests set. */
    public IndexConfig(
        String name,
        String uuid,
        boolean open,
        int numberOfShards,
        int numberOfReplicas,
        int numberOfSearchReplicas,
        Replication replication,
        boolean hidden,
        AllocationEnable allocationEnable,
        long configRevision,
        Map<Integer, Integer> copyOverrides
    ) {
        this(name, uuid, open, numberOfShards, numberOfReplicas, numberOfSearchReplicas, replication, hidden,
            false, allocationEnable, configRevision, copyOverrides);
    }

    /** Everything except the search-replica count, which most callers do not set. */
    public IndexConfig(
        String name,
        String uuid,
        boolean open,
        int numberOfShards,
        int numberOfReplicas,
        Replication replication,
        boolean hidden,
        AllocationEnable allocationEnable,
        long configRevision,
        Map<Integer, Integer> copyOverrides
    ) {
        this(name, uuid, open, numberOfShards, numberOfReplicas, 0, replication, hidden, false, allocationEnable,
            configRevision, copyOverrides);
    }

    /**
     * Everything except the per-shard copy overrides, which almost nothing sets.
     *
     * <p>Kept so that the overrides can be added without touching every construction site — and, more to the
     * point, so that a caller who has no opinion about per-shard scaling does not have to express one.
     */
    public IndexConfig(
        String name,
        String uuid,
        boolean open,
        int numberOfShards,
        int numberOfReplicas,
        Replication replication,
        boolean hidden,
        AllocationEnable allocationEnable,
        long configRevision
    ) {
        this(name, uuid, open, numberOfShards, numberOfReplicas, 0, replication, hidden, false, allocationEnable,
            configRevision, Map.of());
    }

    public IndexConfig {
        copyOverrides = copyOverrides == null ? Map.of() : Map.copyOf(copyOverrides);
        Objects.requireNonNull(name, "name");
        // UNKNOWN here means "the index says nothing", which is different from an unrecognised value and is
        // resolved by falling back to the cluster setting -- see effectiveAllocationEnable.
        allocationEnable = allocationEnable == null ? AllocationEnable.UNKNOWN : allocationEnable;
        Objects.requireNonNull(uuid, "uuid");
        if (numberOfShards < 1) {
            throw new IllegalArgumentException("index [" + name + "] declares " + numberOfShards + " shards");
        }
        if (numberOfReplicas < 0) {
            throw new IllegalArgumentException("index [" + name + "] declares " + numberOfReplicas + " replicas");
        }
        if (numberOfSearchReplicas < 0) {
            throw new IllegalArgumentException(
                "index [" + name + "] declares " + numberOfSearchReplicas + " search replicas");
        }
    }

    /**
     * What actually applies to this index: its own setting if it has one, otherwise the cluster's.
     *
     * <p>Index over cluster, matching OpenSearch, and the reason is asymmetric: an operator freezing one index
     * should not have to know what the cluster-wide value is, while one freezing the cluster expects it to hold
     * everywhere they have not said otherwise.
     */
    public AllocationEnable effectiveAllocationEnable(AllocationEnable clusterValue) {
        return allocationEnable == AllocationEnable.UNKNOWN ? clusterValue : allocationEnable;
    }

    /** Copies desired per shard: one primary plus every kind of replica. */
    public int copiesPerShard() {
        return 1 + desiredCopiesOf(ShardRole.REPLICA) + desiredCopiesOf(ShardRole.SEARCH_REPLICA);
    }

    /**
     * How many copies of one shard should carry a given role — <b>the one place the two settings are mapped onto
     * roles</b>, and the place to change when that mapping does.
     *
     * <p>It is not the obvious mapping, and the reason is worth stating because the obvious one is what OpenSearch
     * does. There, {@code number_of_replicas} means ordinary replicas that peer-recover and can be promoted,
     * while {@code number_of_search_replicas} means read-only copies that never can. Here,
     * {@code number_of_replicas} has always been read through the <em>replication type</em>: on a
     * document-replication index it asks for {@code REPLICA}, and on a segment-replication index it asks for
     * {@code SEARCH_REPLICA}, because a segment-replication copy recovers from the remote store and does not
     * behave like a peer-recovering replica at all.
     *
     * <p>That reading is kept rather than corrected, deliberately, because it is <b>a contract with the data
     * plane rather than a detail of this service</b>. The plugin dispatches on the role it is assigned and not on
     * the replication type: it builds a {@code SEARCH_REPLICA} when a remote store backs the index, and it
     * currently <em>defers</em> a {@code REPLICA} with "a document-replication replica needs peer resolution,
     * which this node does not implement yet". Changing what {@code number_of_replicas} produces would therefore
     * turn working search replicas into deferred ones, which is a change both sides have to make together.
     *
     * <p>The consequence, stated so nobody has to infer it: on a segment-replication index the two settings are
     * <b>added together</b> and both ask for search replicas, so "one ordinary replica and one search replica on
     * a segment-replication index" is still not expressible. See {@code DESIGN.md} §13.
     */
    public int desiredCopiesOf(ShardRole role) {
        return switch (role) {
            case PRIMARY -> 1;
            case REPLICA -> replication == Replication.DOCUMENT ? numberOfReplicas : 0;
            case SEARCH_REPLICA -> replication == Replication.DOCUMENT
                // Search replicas need a remote store, which document replication does not imply. Asking for
                // them here is a contradiction, reported as a finding rather than planned -- see
                // FindingKind.DESIRED_COPY_IGNORED. Counting them would leave the index permanently short of
                // copies nothing could ever place.
                ? 0
                : numberOfSearchReplicas + numberOfReplicas;
            case UNKNOWN -> 0;
        };
    }

    /**
     * Whether this index asks for search replicas that cannot exist, because it does not use segment
     * replication and so has no remote store to recover them from.
     */
    public boolean asksForImpossibleSearchReplicas() {
        return numberOfSearchReplicas > 0 && replication == Replication.DOCUMENT;
    }

    /**
     * How many copies of <em>this shard</em> the pool should be maintaining — the <b>authorised</b> count, as
     * distinct from the configured one.
     *
     * <p>Equal to {@link #copiesPerShard()} unless something has authorised more for this particular shard.
     * Nothing does yet; {@code DESIGN.md} §15 records per-shard replica autoscaling as a goal, and this is the
     * seam it will arrive through.
     *
     * <p>The distinction is worth having now because there is one place where it has teeth: {@code
     * SurplusReaper} deletes copies above this number, one per pass. Anything that raised a single shard's copy
     * count above the index setting would otherwise watch the reaper undo it on the next pass, and the two
     * would burn recovery bandwidth arguing indefinitely.
     *
     * <p>OpenSearch cannot express this at all — {@code index.number_of_replicas} is one number and its routing
     * table is built from it — so it is worth not acquiring that limitation by accident.
     *
     * <p><b>The configured count is a floor.</b> An override below it is ignored rather than honoured: an
     * autoscaler that could reduce redundancy below what an operator asked for is a different and much less
     * welcome feature, and health is still judged against the setting.
     */
    public int copiesAuthorisedFor(int shard) {
        return Math.max(copiesPerShard(), copyOverrides.getOrDefault(shard, 0));
    }

    /** Same index, with extra copies authorised for particular shards. */
    public IndexConfig withCopyOverrides(Map<Integer, Integer> overrides) {
        return new IndexConfig(name, uuid, open, numberOfShards, numberOfReplicas, numberOfSearchReplicas,
            replication, hidden, ingestionPaused, allocationEnable, configRevision, overrides);
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
            intSetting(settings, SETTING_SEARCH_REPLICAS, 0, name),
            // Absent is DOCUMENT, as in OpenSearch (both index.replication.type and the cluster strategy default to
            // it), the control plane, which writes it into every index it creates without one, and the data plane.
            // Reading it as SEGMENT planned search replicas a node then declined (finding 13).
            Replication.from(stringSetting(settings, SETTING_REPLICATION, "DOCUMENT")),
            Boolean.parseBoolean(stringSetting(settings, SETTING_HIDDEN, "false")),
            // From the document body rather than from settings, and absent means consuming.
            root.path(FIELD_INGESTION_STATUS).path(FIELD_IS_PAUSED).asBoolean(false),
            AllocationEnable.from(stringSetting(settings, SETTING_ALLOCATION_ENABLE, null)),
            configRevision,
            Map.of()
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
        return settingsOf(shards, replicas, 0, replication);
    }

    public static Map<String, String> settingsOf(
        int shards,
        int replicas,
        int searchReplicas,
        Replication replication
    ) {
        return Map.of(
            SETTING_SHARDS, Integer.toString(shards),
            SETTING_REPLICAS, Integer.toString(replicas),
            SETTING_SEARCH_REPLICAS, Integer.toString(searchReplicas),
            SETTING_REPLICATION, replication.name().toUpperCase(Locale.ROOT)
        );
    }
}
