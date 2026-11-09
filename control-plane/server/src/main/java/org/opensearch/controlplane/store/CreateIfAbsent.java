/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * A create-if-absent of a value unique to its writer, settled even when the store loses its answer.
 *
 * <p>A write whose answer is lost throws {@link OutcomeUnknownException}: it may or may not have applied. Reading
 * the key settles it when the value is unique to this writer, such as a document carrying a uuid this request
 * generated, because no other writer can have stored a match. So:
 *
 * <ul>
 *   <li>our value is there: the create applied, whichever answer came back;</li>
 *   <li>another value is there: another writer won;</li>
 *   <li>nothing is there: the create did not apply, so it is tried again, with the same value.</li>
 * </ul>
 *
 * <p>A conflict is read back the same way, and has to be: a first attempt whose answer was lost can still apply
 * after the retry has been sent, and the retry then conflicts with its own value. Trying again with the same value
 * is what makes that safe, since both attempts would store the same thing.
 *
 * <p>Not for a value two writers could produce identically, such as an empty marker. Identical bytes from two
 * writers prove nothing, and for those an unknown outcome stays unknown.
 */
public final class CreateIfAbsent {

    /** Attempts before an outcome the store keeps losing is reported as unknown. */
    static final int ATTEMPTS = 3;

    private CreateIfAbsent() {}

    /**
     * @param isOurs whether a stored value is the one this call writes; given the stored bytes
     * @return true if this call's value is stored, false if another writer's is
     * @throws OutcomeUnknownException if the store lost its answer on every attempt and the key is still absent
     */
    public static boolean create(KeyValueStore store, String key, byte[] value, Predicate<byte[]> isOurs) {
        OutcomeUnknownException lost = null;
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            try {
                store.put(key, value, KeyValueStore.ABSENT);
                return true;
            } catch (RevisionConflictException taken) {
                // Something is there; it may be our own earlier attempt.
            } catch (OutcomeUnknownException unknown) {
                lost = unknown;
            }
            Optional<KeyValueStore.Versioned> stored = store.get(key);
            if (stored.isPresent()) {
                return isOurs.test(stored.get().value());
            }
            // Absent: the attempt did not apply, or something else came and went. Try again.
        }
        if (lost != null) {
            throw lost;
        }
        // Every attempt conflicted with a value gone by the time it was read: someone else holds the name in
        // practice, and keeps letting go of it. Reported as the race it is.
        return false;
    }
}
