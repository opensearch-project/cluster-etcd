package org.opensearch.supervisor.model.node;

/**
 * Thrown instead of writing a document that exceeds its size budget.
 *
 * <p>{@code SUPERVISOR_KEYS.md} §5: <b>no document may be truncated to fit</b>. A truncated document parses
 * as a complete one and reads as "the missing things are gone" — a truncated inventory tells the supervisor
 * that shards which exist do not, and it starts replacing them. Failing loudly is the only safe option, and
 * both backends cap value size anyway.
 */
public class DocumentTooLargeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int sizeBytes;
    private final int budgetBytes;

    public DocumentTooLargeException(String what, int sizeBytes, int budgetBytes) {
        super(what + " serialised to " + sizeBytes + " bytes, over its budget of " + budgetBytes
            + "; refusing to write rather than truncate");
        this.sizeBytes = sizeBytes;
        this.budgetBytes = budgetBytes;
    }

    public int sizeBytes() {
        return sizeBytes;
    }

    public int budgetBytes() {
        return budgetBytes;
    }
}
