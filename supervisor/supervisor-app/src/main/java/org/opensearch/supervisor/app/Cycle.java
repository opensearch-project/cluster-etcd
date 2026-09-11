package org.opensearch.supervisor.app;

import org.opensearch.supervisor.core.Supervisor;

/**
 * One turn of the crank, and how to stop cranking — everything {@link Runner} needs from a supervisor.
 *
 * <p>An interface rather than {@code Supervisor} itself, because the two have genuinely different jobs and only
 * one of them has interesting failure behaviour. {@code Runner} decides what happens when a pass throws, when a
 * pass overruns its interval, and when a clean shutdown will not complete — decisions worth testing against a
 * pass that fails on demand, which a real supervisor cannot easily be asked to do. {@code Supervisor} is final,
 * correctly, so this is the seam.
 */
public interface Cycle {

    /** Reconcile once. May throw; the caller decides what that means. */
    void tick();

    /** Give up every lease, so that a clean stop costs no failover window. */
    void shutdown();

    /** A few words for the per-pass debug line. Never throws — it is diagnostics, not logic. */
    String describe();

    /** The real thing. */
    static Cycle of(Supervisor supervisor) {
        return new Cycle() {
            @Override
            public void tick() {
                supervisor.tick();
            }

            @Override
            public void shutdown() {
                supervisor.shutdown();
            }

            @Override
            public String describe() {
                try {
                    return "pass=" + supervisor.pass() + " held=" + supervisor.held().size()
                        + " decision=" + supervisor.lastDecision().getClass().getSimpleName();
                } catch (Throwable thrown) {
                    return "unavailable: " + thrown;
                }
            }
        };
    }
}
