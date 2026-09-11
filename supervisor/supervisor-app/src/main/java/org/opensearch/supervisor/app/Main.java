package org.opensearch.supervisor.app;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStores;
import org.opensearch.supervisor.core.Supervisor;
import org.opensearch.supervisor.model.Keys;

import java.time.Clock;

/**
 * Entry point: turn arguments into a store, a supervisor and a loop, and get out of the way.
 *
 * <p>Deliberately thin. Everything decidable is decided in {@code supervisor-core}, which is where the tests
 * are; this file's only real content is the order things happen in and what a failure at each step should look
 * like to somebody watching a process that will not start.
 */
public final class Main {

    private static final Log log = Log.of("main");

    private Main() {}

    public static void main(String[] args) {
        System.exit(run(args));
    }

    /** Separated from {@link #main} so that a test can run it without ending the JVM. */
    static int run(String[] args) {
        Options options;
        try {
            options = Options.parse(args);
        } catch (Options.UsageException e) {
            // Asking for help is not a failure and must not look like one to a shell.
            (e.requested() ? System.out : System.err).println(e.getMessage());
            return e.requested() ? 0 : 2;
        }

        Log.threshold(options.logLevel());
        log.info("starting", options.describe().toArray());
        if (options.readOnly()) {
            log.info("read-only: this process will observe, diff and project, but will not write any assignment."
                + " Pass --write when you want it to act.");
        } else {
            log.warn("write mode: this process will create and remove shard copies on data nodes");
        }

        try (KeyValueStore store = open(options)) {
            Supervisor supervisor = new Supervisor(
                store,
                Clock.systemUTC(),
                options.supervisorId(),
                options.tenants(),
                options.capacityWeight(),
                options.leaseTtl(),
                options.balancer(),
                options.policy(),
                options.readOnly()
            );

            warnIfIdentityLooksTaken(store, options);

            Runner runner = new Runner(Cycle.of(supervisor), options);
            // Registered before the loop starts, so a signal arriving during the first pass is still honoured.
            Runtime.getRuntime().addShutdownHook(new Thread(runner::stop, "supervisor-signal"));
            log.info("running", "interval_ms", options.interval().toMillis());
            return runner.run();
        } catch (Options.UsageException e) {
            System.err.println(e.getMessage());
            return 2;
        } catch (Throwable thrown) {
            // Everything from here is fatal-at-startup: no store, no provider, an unusable URI. The loop's own
            // failures never reach this point, because a pass that throws is caught and retried.
            log.error("could not start", thrown, "store", options.storeUri());
            return 1;
        }
    }

    private static KeyValueStore open(Options options) throws java.io.IOException {
        try {
            KeyValueStore store = KeyValueStores.open(options.storeUri());
            log.info("opened store", "store", options.storeUri(), "impl", store.getClass().getName());
            return store;
        } catch (java.io.IOException e) {
            // The most likely first-run failure by a wide margin, and the message that helps is the list of
            // schemes actually on the classpath -- a missing backend jar looks identical to a typo otherwise.
            throw new java.io.IOException(e.getMessage() + "\n\n" + KeyValueStores.describeAvailable(), e);
        }
    }

    /**
     * Warns if something already holds this process's identity.
     *
     * <p>Two supervisors sharing an id would each believe they hold the same pool and both write assignments for
     * it, which is the one failure the pool partition model cannot absorb. This is not a proof of collision —
     * telling a live peer from a record left by an unclean shutdown needs two observations separated by time,
     * and delaying startup to get them would be a poor trade. So it is a warning with the two possibilities
     * named, which is enough for somebody who has just copied a deployment template.
     */
    private static void warnIfIdentityLooksTaken(KeyValueStore store, Options options) {
        try {
            if (store.get(Keys.member(options.supervisorId())).isPresent()) {
                log.warn("a member record already exists for this id. Either a previous process with this id did"
                        + " not shut down cleanly, which is harmless and clears itself, or another process is"
                        + " running with the same id, which is not: two supervisors sharing an id will both"
                        + " write to the same pool. Check --id.",
                    "id", options.supervisorId());
            }
        } catch (Throwable thrown) {
            // A store that cannot answer this is a store the first pass will complain about in more detail.
            log.debug("could not check for an existing member record", "cause", thrown.toString());
        }
    }
}
