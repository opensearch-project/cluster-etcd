/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.rest.Json;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The store's limits, checked before writing, so that a request over them is refused as a client error
 * naming the resource rather than failing at the store halfway through.
 *
 * <ul>
 *   <li><b>Names</b> that become a key segment — index, data stream, alias, template, pipeline, stored
 *       script and repository names — are at most {@link #MAX_NAME_BYTES} UTF-8 bytes. That is OpenSearch's
 *       limit on index and alias names, and it is what keeps the longest key, a 128-byte tenant plus a
 *       resource prefix plus the name, inside {@link KeyValueStore#MAX_KEY_BYTES}.</li>
 *   <li><b>Documents</b> are at most {@link KeyValueStore#MAX_VALUE_BYTES} serialised. An operation that
 *       writes several checks all of them before writing any, so it is refused whole.</li>
 * </ul>
 *
 * <p>The store enforces both itself, with {@link LimitExceededException}, and {@code Router} answers that
 * with a 400. These checks exist for the messages and for the multi-write operations, not as the only line.
 */
public final class WriteLimits {

    public static final int MAX_NAME_BYTES = 255;

    private WriteLimits() {}

    public static int utf8Bytes(String name) {
        return name.getBytes(StandardCharsets.UTF_8).length;
    }

    /** {@code (N > 255)} if the name is over the limit, the way OpenSearch states its index-name limit. */
    public static Optional<String> nameTooLong(String name) {
        int bytes = utf8Bytes(name);
        return bytes > MAX_NAME_BYTES
            ? Optional.of("(" + bytes + " > " + MAX_NAME_BYTES + ")")
            : Optional.empty();
    }

    /** The document's serialised size, if that is over the store's value limit. */
    public static OptionalInt oversized(JsonNode document) {
        int bytes = Json.toBytes(document).length;
        return bytes > KeyValueStore.MAX_VALUE_BYTES ? OptionalInt.of(bytes) : OptionalInt.empty();
    }
}
