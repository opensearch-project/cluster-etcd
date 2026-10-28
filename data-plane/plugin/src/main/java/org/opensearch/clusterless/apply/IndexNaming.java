/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.TenantId;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

/**
 * The name a fabricated index carries inside this node's {@link org.opensearch.cluster.ClusterState}.
 *
 * <p>The store is multi-tenant and identifies an index by {@code (tenant, uuid)}. A {@code ClusterState} has
 * one flat index namespace keyed by name, and index names are only unique within a tenant — so two tenants
 * with a {@code logs} index would collide. Something has to give.
 *
 * <p><b>What this name is not.</b> It is not the identity. That is carried in index settings — the real uuid in
 * {@code index.uuid}, the tenant in {@code index.clusterless.tenant}, the tenant-scoped name in
 * {@code index.clusterless.name} — so nothing reads anything back out of this string, and
 * {@link org.opensearch.core.index.Index} equality inside the server is already identity in the contract's
 * sense. That is what frees the name to be chosen for legibility instead of for parsing.
 *
 * <p>So it needs three properties and no more:
 *
 * <ul>
 *   <li><b>Unique across tenants.</b> Hence the fingerprint: sanitising alone would map tenants {@code Acme}
 *       and {@code acme} onto one name, and index names are lower-case while tenant ids and uuids are not.
 *   <li><b>Stable across restarts and across nodes.</b> Hence a hash of the identity rather than anything
 *       counted, remembered or timed. Two nodes holding copies of one shard must agree on the name, and a
 *       restarted node must agree with itself — a name that moved would look to the server like an index
 *       being dropped and a different one created.
 *   <li><b>A legal index name.</b> Lower-case, none of {@code \/*?"<>| ,#:}, not starting {@code -}, {@code _}
 *       or {@code +}, under 255 bytes. The tenant leads, and a {@link TenantId} must start alphanumeric, so
 *       the result cannot begin with a character the server refuses — including the leading dot that a data
 *       stream's backing index has.
 * </ul>
 *
 * <p>Shard data on disk is keyed by index uuid rather than by name, so a name is not a durability concern:
 * changing this scheme would churn cluster state, not lose bytes. It would still be a compatibility break for
 * any running node, since the two names would look like two indices.
 */
public final class IndexNaming {

    /** Enough of the digest to make a collision irrelevant, and short enough to read. Forty bits. */
    static final int FINGERPRINT_HEX_CHARS = 10;

    /** Bounds on the readable parts, so the whole name stays well under the server's 255-byte limit. */
    static final int MAX_TENANT_CHARS = 48;

    static final int MAX_INDEX_CHARS = 128;

    private IndexNaming() {}

    /**
     * @param indexName the index's name within its tenant, used only to make the result readable
     */
    public static String indexName(TenantId tenant, String uuid, String indexName) {
        return sanitise(tenant.value(), MAX_TENANT_CHARS) + "." + sanitise(indexName, MAX_INDEX_CHARS) + "." + fingerprint(tenant, uuid);
    }

    /**
     * Lower-cased, with everything the server would refuse replaced by a hyphen.
     *
     * <p>Not reversible, and deliberately not: an escaping scheme that could be decoded would be a parser, and
     * a parser here would invite reading identity back out of the name — which is the mistake this design
     * avoids by putting identity in settings.
     */
    private static String sanitise(String value, int maxChars) {
        String lower = value.toLowerCase(Locale.ROOT);
        StringBuilder sanitised = new StringBuilder(Math.min(lower.length(), maxChars));
        for (int i = 0; i < lower.length() && sanitised.length() < maxChars; i++) {
            char character = lower.charAt(i);
            boolean legal = (character >= 'a' && character <= 'z')
                || (character >= '0' && character <= '9')
                || character == '-'
                || character == '_'
                || character == '.';
            sanitised.append(legal ? character : '-');
        }
        return sanitised.length() == 0 ? "x" : sanitised.toString();
    }

    /**
     * A fingerprint of the identity, and of the identity only.
     *
     * <p>Over {@code (tenant, uuid)} with a separator that cannot occur in either, so that no pair of
     * identities can produce one string to hash — {@code ("ab", "c")} and {@code ("a", "bc")} must not collide,
     * and concatenation alone would let them.
     */
    private static String fingerprint(TenantId tenant, String uuid) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.update(tenant.value().getBytes(StandardCharsets.UTF_8));
            sha256.update((byte) 0);
            sha256.update(uuid.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(sha256.digest()).substring(0, FINGERPRINT_HEX_CHARS);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every JVM", e);
        }
    }
}
