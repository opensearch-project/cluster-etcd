package org.opensearch.supervisor.model.node;

/**
 * Names for the optional parts of {@code NODE_CONTRACT.md}, as a node declares them in its registration.
 *
 * <p>Strings rather than an enum, deliberately: a node newer than this supervisor declares features this build has no
 * name for, and those must survive the round trip and be ignored rather than failing the parse or being dropped.
 */
public final class ContractFeatures {

    /**
     * The node honours {@code primary_terms}. Required on every node involved before the supervisor promotes or moves
     * a primary, because a node that ignored the field would fabricate term 1 and report {@code applied} anyway.
     */
    public static final String PRIMARY_TERMS = "primary_terms";

    private ContractFeatures() {}
}
