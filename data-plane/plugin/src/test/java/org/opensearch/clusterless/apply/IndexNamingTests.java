/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

/**
 * The name a fabricated index carries, and the three things it has to be.
 *
 * <p>Worth testing despite being a string concatenation, because two of the three properties are invisible until
 * they are violated in production: a name that is not stable looks like an index being dropped and a different
 * one created, and a name that is not unique makes two tenants' indices the same index.
 */
public class IndexNamingTests extends OpenSearchTestCase {

    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    public void testTheNameIsReadable() {
        // The whole reason not to use the uuid, which would be unique and stable and useless in a log.
        String name = IndexNaming.indexName(TenantId.of("acme"), UUID, "my-index");
        assertTrue(name, name.startsWith("acme.my-index."));
    }

    public void testTheNameIsStable() {
        // Two nodes holding copies of one shard must agree, and a restarted node must agree with itself.
        assertEquals(
            IndexNaming.indexName(TenantId.of("acme"), UUID, "my-index"),
            IndexNaming.indexName(TenantId.of("acme"), UUID, "my-index")
        );
    }

    public void testTheNameDependsOnIdentityAndNotOnTheReadablePart() {
        // The fingerprint is over (tenant, uuid). A renamed index -- same uuid, different name -- keeps its
        // fingerprint, and a recreated one -- same name, new uuid -- does not.
        String original = IndexNaming.indexName(TenantId.of("acme"), UUID, "my-index");
        String renamed = IndexNaming.indexName(TenantId.of("acme"), UUID, "renamed");
        String recreated = IndexNaming.indexName(TenantId.of("acme"), "aDifferentUuid123456", "my-index");

        assertEquals(fingerprintOf(original), fingerprintOf(renamed));
        assertNotEquals(fingerprintOf(original), fingerprintOf(recreated));
        assertNotEquals(original, recreated);
    }

    public void testTenantsThatSanitiseToTheSameStringStillGetDifferentNames() {
        // Index names are lower-case and tenant ids are not, so sanitising alone maps these onto one name. This
        // is the collision the fingerprint exists for.
        Set<String> names = new HashSet<>();
        names.add(IndexNaming.indexName(TenantId.of("Acme"), UUID, "i"));
        names.add(IndexNaming.indexName(TenantId.of("acme"), UUID, "i"));
        names.add(IndexNaming.indexName(TenantId.of("ACME"), UUID, "i"));

        assertEquals("each tenant must get its own index name", 3, names.size());
    }

    public void testTheFingerprintCannotBeConfusedByWhereTheTenantEnds() {
        // ("ab", "c") and ("a", "bc") hash to the same thing under plain concatenation. The separator is why
        // they do not here.
        assertNotEquals(
            IndexNaming.indexName(TenantId.of("ab"), "c", "i"),
            IndexNaming.indexName(TenantId.of("a"), "bc", "i")
        );
    }

    public void testEveryNameIsLegalForAnIndex() {
        // Lower-case, none of the reserved characters, and not starting with one of the three the server
        // refuses. A data stream's backing index starts with a dot, which is why the tenant leads.
        assertLegal(IndexNaming.indexName(TenantId.of("acme"), UUID, ".ds-logs-app-000001"));
        assertLegal(IndexNaming.indexName(TenantId.of("Acme.Corp_1"), UUID, "MY Index/With:Bad*Chars"));
        assertLegal(IndexNaming.indexName(TenantId.of("a"), UUID, ""));
        assertLegal(IndexNaming.indexName(TenantId.of("9lives"), UUID, "_leading"));
    }

    public void testEvenAbsurdInputsStayWellUnderTheLengthLimit() {
        String name = IndexNaming.indexName(TenantId.of("t".repeat(128)), UUID, "i".repeat(400));

        assertTrue(name, name.length() <= IndexNaming.MAX_TENANT_CHARS + IndexNaming.MAX_INDEX_CHARS + IndexNaming.FINGERPRINT_HEX_CHARS + 2);
        assertTrue(name, name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 255);
        assertLegal(name);
    }

    public void testTwoIndicesOfOneTenantDoNotCollide() {
        assertNotEquals(
            IndexNaming.indexName(TenantId.of("acme"), "uuid-one-aaaaaaaaaa", "logs"),
            IndexNaming.indexName(TenantId.of("acme"), "uuid-two-bbbbbbbbbb", "logs")
        );
    }

    private static String fingerprintOf(String name) {
        return name.substring(name.length() - IndexNaming.FINGERPRINT_HEX_CHARS);
    }

    private static void assertLegal(String name) {
        assertFalse(name, name.isEmpty());
        assertEquals("must be lower case", name.toLowerCase(java.util.Locale.ROOT), name);
        for (char reserved : new char[] { '\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':' }) {
            assertFalse(name + " contains " + reserved, name.indexOf(reserved) >= 0);
        }
        assertFalse("must not start with - _ or +", name.startsWith("-") || name.startsWith("_") || name.startsWith("+"));
        assertFalse("must not be . or ..", name.equals(".") || name.equals(".."));
        // A leading dot makes an index hidden, which this one is not.
        assertFalse("must not start with a dot", name.startsWith("."));
    }
}
