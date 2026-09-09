package org.opensearch.supervisor.model.node;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.opensearch.supervisor.model.TenantId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Reads and writes the four documents of {@code NODE_CONTRACT.md}.
 *
 * <p>Both sides of the seam use this class — the supervisor and the data plane node — so that there is one
 * encoding rather than two that agree until they do not. That is the same argument the store itself is
 * shared for.
 */
public final class NodeJson {

    /** Nothing in the heartbeat scales, so this can only be exceeded by a bug. */
    public static final int HEARTBEAT_BUDGET_BYTES = 4 * 1024;

    public static final int REGISTRATION_BUDGET_BYTES = 4 * 1024;

    /** Well clear of ZooKeeper's 1 MiB default and etcd's 1.5 MiB. Several thousand copies per node. */
    public static final int INVENTORY_BUDGET_BYTES = 512 * 1024;

    public static final int ASSIGNMENT_BUDGET_BYTES = 512 * 1024;

    private static final ObjectMapper MAPPER = createMapper();

    private NodeJson() {}

    private static ObjectMapper createMapper() {
        SimpleModule module = new SimpleModule();
        // TenantId is a map key in three of the four documents. @JsonValue covers it as a value; keys go
        // through a separate path and need this.
        module.addKeyDeserializer(TenantId.class, new KeyDeserializer() {
            @Override
            public Object deserializeKey(String key, com.fasterxml.jackson.databind.DeserializationContext ctxt) {
                return TenantId.of(key);
            }
        });

        registerWireEnum(module, NodeRole.class, NodeRole::from);
        registerWireEnum(module, ShardState.class, ShardState::from);
        registerWireEnum(module, AppliedOutcome.class, AppliedOutcome::from);

        return JsonMapper.builder()
            .addModule(module)
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .defaultPropertyInclusion(
                JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS)
            )
            // A node newer than this supervisor will write fields this build has never heard of, and a
            // rolling upgrade means that is the normal case rather than the exceptional one. Failing the
            // parse would lose the whole document — and for an inventory, losing the document reads as
            // "this node has no shards", which is the most destructive possible misreading.
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            // Deterministic output. The heartbeat carries a digest of the exact bytes of the inventory, so
            // a node that re-serialised an unchanged inventory into differently ordered JSON would announce
            // a new digest and make the supervisor re-read something that had not changed.
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .build();
    }

    /**
     * Explicit serializer and deserializer rather than {@code @JsonValue} and {@code @JsonCreator}.
     *
     * <p>Jackson builds an enum's deserializer by calling the {@code @JsonValue} accessor on every constant
     * to construct its lookup table, so an {@code UNKNOWN} constant that throws on write also breaks
     * reading — defeating the one thing it exists for. A registered serializer runs only when something
     * genuinely tries to write the sentinel, which is where the guard belongs.
     */
    private static <E extends Enum<E> & WireEnum> void registerWireEnum(
        SimpleModule module,
        Class<E> type,
        java.util.function.Function<String, E> parser
    ) {
        module.addSerializer(type, new JsonSerializer<E>() {
            @Override
            public void serialize(E value, JsonGenerator generator, SerializerProvider provider) throws IOException {
                generator.writeString(value.wireName());
            }
        });
        module.addDeserializer(type, new JsonDeserializer<E>() {
            @Override
            public E deserialize(JsonParser parser0, DeserializationContext context) throws IOException {
                return parser.apply(parser0.getValueAsString());
            }
        });
    }


    // ---------------------------------------------------------------- writing

    public static byte[] write(Registration registration) {
        return writeChecked(registration, "registration", REGISTRATION_BUDGET_BYTES);
    }

    public static byte[] write(Heartbeat heartbeat) {
        return writeChecked(heartbeat, "heartbeat", HEARTBEAT_BUDGET_BYTES);
    }

    public static byte[] write(ShardInventory inventory) {
        return writeChecked(inventory, "shard inventory", INVENTORY_BUDGET_BYTES);
    }

    public static byte[] write(Assignment assignment) {
        return writeChecked(assignment, "assignment", ASSIGNMENT_BUDGET_BYTES);
    }

    private static byte[] writeChecked(Object document, String what, int budget) {
        byte[] bytes;
        try {
            bytes = MAPPER.writeValueAsBytes(document);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("could not serialise " + what, e);
        }
        if (bytes.length > budget) {
            throw new DocumentTooLargeException(what, bytes.length, budget);
        }
        return bytes;
    }

    // ---------------------------------------------------------------- reading

    public static Registration readRegistration(byte[] bytes) {
        return read(bytes, Registration.class, "registration");
    }

    public static Heartbeat readHeartbeat(byte[] bytes) {
        return read(bytes, Heartbeat.class, "heartbeat");
    }

    public static ShardInventory readInventory(byte[] bytes) {
        return read(bytes, ShardInventory.class, "shard inventory");
    }

    public static Assignment readAssignment(byte[] bytes) {
        return read(bytes, Assignment.class, "assignment");
    }

    private static <T> T read(byte[] bytes, Class<T> type, String what) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (JsonMappingException e) {
            throw new MalformedDocumentException(what, e);
        } catch (IOException e) {
            throw new MalformedDocumentException(what, e);
        }
    }

    /**
     * A document under our prefix that could not be parsed at all.
     *
     * <p>Distinct from {@link DocumentTooLargeException} and thrown rather than swallowed here, because the
     * caller is the only one who knows what to do: R2.5 says a single unreadable key must not stop a pass,
     * so the observation layer catches this per node and raises a finding, while a node reading its own
     * assignment should treat it as no assignment rather than as an empty one.
     */
    public static final class MalformedDocumentException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        MalformedDocumentException(String what, Throwable cause) {
            super("could not parse " + what + ": " + cause.getMessage(), cause);
        }
    }

    // ---------------------------------------------------------------- the inventory digest

    /**
     * The digest the heartbeat announces: hex SHA-256 of the <b>exact bytes</b> written to
     * {@code nodes/shards/<node-id>}, prefixed with the algorithm.
     *
     * <p>Over bytes rather than over a canonical form on purpose. The supervisor digests the bytes it read
     * and compares them with what the heartbeat claims, so neither side ever has to agree on a canonical
     * serialisation — only on which bytes were stored. That also means this stays correct if the encoding
     * changes.
     */
    public static String digest(byte[] documentBytes) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return "sha256:" + HexFormat.of().formatHex(sha256.digest(documentBytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every JVM", e);
        }
    }

    /** For diagnostics and tests. */
    public static String toJson(Object document) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(document);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("could not serialise", e);
        }
    }

    static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
