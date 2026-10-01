package org.opensearch.supervisor.model.diff;

/** How much a finding matters, which decides ordering and what survives truncation of the diff document. */
public enum Severity {
    /** Expected during convergence. Becomes interesting only with age. */
    INFO,
    WARNING,
    CRITICAL
}
