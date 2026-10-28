package org.opensearch.supervisor.app;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.core.MonitorReader;
import org.opensearch.supervisor.model.monitor.Alert;
import org.opensearch.supervisor.model.monitor.Rules;
import org.opensearch.supervisor.model.monitor.Sample;

import java.time.Clock;
import java.util.List;

/**
 * {@code supervisor-app alerts} — read the store once, print what is wrong, exit non-zero if anything is.
 *
 * <p><b>Exit status is the interface.</b> {@code 0} clean, {@code 1} a warning fired, {@code 2} a critical fired,
 * {@code 3} the store could not be read. That is enough for anything that can run a command — cron, a CI job, a
 * terminal during a demo — and it needs no agreement with anybody about an alert schema, no webhook, and no
 * credentials. Everything harder about alerting (dedupe, suppression windows, who gets paged) is deferred until
 * somebody has a rotation to answer those questions for.
 *
 * <p>It writes nothing, structurally: the rules live in {@code supervisor-model}, which cannot see a store, and
 * {@link MonitorReader} is the only class in the path that can. So this is safe to point at production on day one.
 *
 * <p><b>One-shot has a real limitation, stated rather than hidden.</b> Two of the rules need two observations —
 * "this pool's pass number has not moved" is the alert this whole component exists for — and a single invocation
 * has only one. Run it twice with {@code --watch}, or from cron often enough that the second run has a predecessor;
 * a lone run simply does not fire them, and says so.
 */
final class Alerts {

    /** The store could not be read at all, which is distinct from "nothing is wrong". */
    static final int EXIT_UNREADABLE = 3;

    private static final Log log = Log.of("alerts");

    private Alerts() {}

    static int run(KeyValueStore store, Options options) {
        MonitorReader reader = new MonitorReader(store, Clock.systemUTC());

        Sample sample;
        try {
            sample = reader.read(null);
            if (options.alertSamples() > 1) {
                // A second look, so the rules that need a predecessor can fire. The gap has to be long enough for
                // a pass to have happened, or "the pass number has not moved" is just two reads in quick
                // succession -- which is why Rules has a floor and why this waits rather than reading twice fast.
                for (int taken = 1; taken < options.alertSamples(); taken++) {
                    log.info("waiting before the next sample so a pass can happen",
                        "wait_ms", options.alertInterval().toMillis());
                    Thread.sleep(options.alertInterval().toMillis());
                    sample = reader.read(sample);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return EXIT_UNREADABLE;
        } catch (Throwable thrown) {
            log.error("could not read the store", thrown, "store", options.storeUri());
            return EXIT_UNREADABLE;
        }

        List<Alert> alerts = Rules.evaluate(sample);

        if (options.alertJson()) {
            System.out.println(org.opensearch.supervisor.model.Json.toJson(alerts));
        } else {
            print(sample, alerts);
        }

        return alerts.stream().mapToInt(Alert::exitStatus).max().orElse(0);
    }

    /**
     * One line per alert, severity first, carrying the supervisor's own sentence verbatim.
     *
     * <p>A formatter is not a feature, so there are exactly two forms and this is the one for a human. The
     * "nothing is wrong" case prints something rather than nothing, because silence from a monitoring command is
     * indistinguishable from a monitoring command that did not run — which is the failure this entire component
     * exists to notice, and it would be embarrassing to reproduce it in the output.
     */
    private static void print(Sample sample, List<Alert> alerts) {
        if (alerts.isEmpty()) {
            System.out.println("OK  nothing to report across " + sample.poolsDefined().size() + " pool(s), "
                + sample.diffs().size() + " tenant diff(s), " + sample.liveMembers().size() + " live member(s)");
            if (sample.previous() == null) {
                System.out.println("    note: one sample only, so the rules needing two observations did not run."
                    + " Pass --alert-samples 2 to include them.");
            }
            return;
        }
        for (Alert alert : alerts) {
            System.out.printf("%-8s %-28s %-28s %s%n",
                alert.severity(), alert.rule(), alert.subject(), alert.because());
        }
    }
}
