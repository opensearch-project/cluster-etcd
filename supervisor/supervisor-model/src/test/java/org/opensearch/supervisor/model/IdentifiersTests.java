package org.opensearch.supervisor.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class IdentifiersTests {

    @ParameterizedTest
    @ValueSource(strings = { "default", "acme", "a", "A1", "with_underscore", "with.dot", "with-dash" })
    void tenantIdsTheControlPlaneAcceptsAreAccepted(String value) {
        assertEquals(value, TenantId.of(value).value());
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "_leading", ".leading", "-leading", "has/slash", "has space", "has:colon" })
    void tenantIdsTheControlPlaneRejectsAreRejected(String value) {
        assertThrows(IllegalArgumentException.class, () -> TenantId.of(value));
    }

    @Test
    void tenantIdLengthMatchesTheControlPlaneExactly() {
        // 128 is the control plane's limit. A tenant it can serve and this service cannot represent is a
        // tenant that goes silently unmanaged, so the two patterns have to move together.
        assertEquals(128, TenantId.of("a".repeat(128)).value().length());
        assertThrows(IllegalArgumentException.class, () -> TenantId.of("a".repeat(129)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "pv1sDMfZRxSTMiW5VrbN9w", "a", "_x", "-x", "A_1-2" })
    void nodeIdsAreOpenSearchShaped(String value) {
        assertEquals(value, NodeId.of(value).value());
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "has/slash", "has.dot", "has space" })
    void nodeIdsRejectAnythingThatIsNotBase64Url(String value) {
        assertThrows(IllegalArgumentException.class, () -> NodeId.of(value));
    }

    @ParameterizedTest
    @ValueSource(strings = { "shared-a", "dedicated.acme", "p1" })
    void poolIdsAreAccepted(String value) {
        assertEquals(value, PoolId.of(value).value());
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "-leading", "has/slash" })
    void poolIdsRejectLeadingPunctuationAndSlashes(String value) {
        assertThrows(IllegalArgumentException.class, () -> PoolId.of(value));
    }

    @Test
    void identifiersOfDifferentKindsAreNotInterchangeable() {
        // Records with the same component would otherwise compare equal through Object.equals if anything
        // ever put them in the same collection. They are different things and the type system should say so.
        assertNotEquals((Object) TenantId.of("x"), (Object) PoolId.of("x"));
        assertEquals(TenantId.of("x"), TenantId.of("x"));
    }

    @Test
    void identifiersSortForStableIteration() {
        // Ordering matters for the "arbitrary but stable" total order the task lifecycle needs: sequence
        // restarts per control plane process and submitted_at ties constantly, so neither can supply one.
        assertEquals(-1, Integer.signum(TenantId.of("a").compareTo(TenantId.of("b"))));
        assertEquals(0, TenantId.of("a").compareTo(TenantId.of("a")));
    }

    @Test
    void nullIsRejectedEverywhere() {
        assertThrows(NullPointerException.class, () -> TenantId.of(null));
        assertThrows(NullPointerException.class, () -> NodeId.of(null));
        assertThrows(NullPointerException.class, () -> PoolId.of(null));
    }
}
