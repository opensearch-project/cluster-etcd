package org.opensearch.supervisor.model;

/**
 * An enum with a defined representation in {@code NODE_CONTRACT.md}, and an {@code UNKNOWN} constant for
 * values a newer peer may send.
 *
 * <p>The wire name is produced here rather than by {@code @JsonValue} for a reason worth recording:
 * Jackson builds an enum's <em>deserializer</em> by calling the {@code @JsonValue} accessor on every
 * constant to construct its lookup table, so an {@code UNKNOWN} that throws on write also breaks reading —
 * which is the one thing it exists to protect. Registering a serializer instead means the guard fires only
 * when something genuinely tries to write the sentinel.
 */
public interface WireEnum {

    /**
     * @throws IllegalStateException if this is the {@code UNKNOWN} sentinel. Writing it would fabricate a
     *     document claiming a value no peer ever reported.
     */
    String wireName();
}
