/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

/**
 * The checks every backend runs before a write, so that {@link KeyValueStore#MAX_KEY_BYTES} and
 * {@link KeyValueStore#MAX_VALUE_BYTES} are enforced identically everywhere.
 *
 * <p>Enforced even by a backend that could store more. A consumer that works against etcd must work against
 * ZooKeeper, so the limit is the common one rather than whatever the backend in use happens to allow.
 */
public final class StoreLimits {

    private StoreLimits() {}

    /** @throws LimitExceededException if the key or value is over its limit */
    public static void checkWrite(String key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        int keyBytes = key.getBytes(StandardCharsets.UTF_8).length;
        if (keyBytes > KeyValueStore.MAX_KEY_BYTES) {
            throw new LimitExceededException(String.format(
                Locale.ROOT, "key [%s...] is %d UTF-8 bytes, over the limit of %d",
                key.substring(0, Math.min(key.length(), 64)), keyBytes, KeyValueStore.MAX_KEY_BYTES));
        }
        if (value.length > KeyValueStore.MAX_VALUE_BYTES) {
            throw new LimitExceededException(String.format(
                Locale.ROOT, "value for key [%s] is %d bytes, over the limit of %d",
                key, value.length, KeyValueStore.MAX_VALUE_BYTES));
        }
    }
}
