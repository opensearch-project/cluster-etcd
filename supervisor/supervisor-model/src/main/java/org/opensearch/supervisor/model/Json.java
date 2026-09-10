package org.opensearch.supervisor.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.opensearch.supervisor.model.node.AppliedOutcome;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.status.HealthStatus;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;
import org.opensearch.supervisor.model.settings.AllocationEnable;
import org.opensearch.supervisor.model.task.TaskState;
import org.opensearch.supervisor.model.task.TaskType;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.function.Function;

/**
 * One JSON encoding for every document this system writes.
 *
 * <p>Both sides of every seam use it — the supervisor, the data plane node, and the control plane where it
 * reads a projection — so that there is one encoding rather than two that agree until they do not. It is
 * the same argument the key/value store itself is shared for.
 */
public final class Json {

    private static final ObjectMapper MAPPER = createMapper();

    private Json() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    private static ObjectMapper createMapper() {
        SimpleModule module = new SimpleModule();

        // TenantId is a map key in several documents. @JsonValue covers it as a value; keys go through a
        // separate path and need this.
        module.addKeyDeserializer(TenantId.class, new KeyDeserializer() {
            @Override
            public Object deserializeKey(String key, DeserializationContext context) {
                return TenantId.of(key);
            }
        });

        registerWireEnum(module, NodeRole.class, NodeRole::from);
        registerWireEnum(module, ShardRole.class, ShardRole::from);
        registerWireEnum(module, ShardState.class, ShardState::from);
        registerWireEnum(module, AppliedOutcome.class, AppliedOutcome::from);
        registerWireEnum(module, Replication.class, Replication::from);
        registerWireEnum(module, HealthStatus.class, HealthStatus::from);
        registerWireEnum(module, Liveness.class, Liveness::from);
        registerWireEnum(module, PoolKind.class, PoolKind::from);
        registerWireEnum(module, TaskState.class, TaskState::from);
        registerWireEnum(module, TaskType.class, TaskType::from);
        registerWireEnum(module, AllocationEnable.class, AllocationEnable::from);

        return JsonMapper.builder()
            .addModule(module)
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .defaultPropertyInclusion(
                JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS)
            )
            // A peer newer than this build will write fields it has never heard of, and a rolling upgrade
            // makes that the normal case rather than the exceptional one. Failing the parse would lose the
            // whole document — and for a shard inventory, losing the document reads as "this node has no
            // shards", which is the most destructive possible misreading.
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            // Deterministic output. A node's heartbeat carries a digest of the exact bytes of its
            // inventory, so re-serialising an unchanged inventory into differently ordered JSON would
            // announce a new digest and make the supervisor re-read something that had not changed.
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
        Function<String, E> parser
    ) {
        module.addSerializer(type, new JsonSerializer<E>() {
            @Override
            public void serialize(E value, JsonGenerator generator, SerializerProvider provider) throws IOException {
                generator.writeString(value.wireName());
            }
        });
        module.addDeserializer(type, new JsonDeserializer<E>() {
            @Override
            public E deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
                return parser.apply(jsonParser.getValueAsString());
            }
        });
    }

    public static byte[] write(Object document, String what) {
        try {
            return MAPPER.writeValueAsBytes(document);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("could not serialise " + what, e);
        }
    }

    public static <T> T read(byte[] bytes, Class<T> type, String what) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (IOException e) {
            throw new MalformedDocumentException(what, e);
        }
    }

    /** Pretty-printed, for diagnostics and tests. */
    public static String toJson(Object document) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(document);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("could not serialise", e);
        }
    }

    /**
     * Hex SHA-256 of the <b>exact bytes</b> of a stored document, prefixed with the algorithm.
     *
     * <p>Over bytes rather than a canonical form on purpose: the reader digests what it read and compares
     * it with what the writer claimed, so neither side has to agree on a canonical serialisation — only on
     * which bytes were stored. That also keeps it correct if the encoding changes.
     */
    public static String digest(byte[] documentBytes) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return "sha256:" + HexFormat.of().formatHex(sha256.digest(documentBytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every JVM", e);
        }
    }

    /**
     * A document that could not be parsed at all.
     *
     * <p>Thrown rather than swallowed, because only the caller knows what to do: R2.5 says a single
     * unreadable key must not stop a pass, so the observation layer catches this per node and raises a
     * finding, while a node reading its own assignment treats it as no assignment rather than an empty one.
     */
    public static final class MalformedDocumentException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        MalformedDocumentException(String what, Throwable cause) {
            super("could not parse " + what + ": " + cause.getMessage(), cause);
        }
    }
}
