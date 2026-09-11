package org.opensearch.supervisor.app;

import org.opensearch.controlplane.store.KeyValueStores;
import org.opensearch.supervisor.core.Balancer;
import org.opensearch.supervisor.core.LivenessTracker;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.plan.Planner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * What the process was told to do. Parsed once, at startup, and then immutable.
 *
 * <p>Two defaults here are safety decisions rather than conveniences, and both are the opposite of what is
 * convenient.
 *
 * <p><b>Read-only unless {@code --write} is given.</b> This service's job includes deleting shard copies. A
 * process that starts writing because somebody forgot a flag is a worse outcome than one that starts reporting
 * because somebody forgot a flag, and the asymmetry is large: the second is a wasted afternoon and the first can
 * move a cluster's data. Read-only is also the sanctioned way to introduce this to a cluster that already has
 * data in it — observe, diff and project for a while, read what it claims it would do, then let it act.
 *
 * <p><b>The identity must be unique per process, so the default is unique rather than stable.</b> Leases record
 * their holder by supervisor id, so two processes sharing an id would each believe they hold the same pool, and
 * would both write assignments for it — precisely the split brain the pool partition model exists to make
 * impossible. Stability across restarts would be nicer operationally (a restarted process would reclaim its own
 * member record instead of leaving one to be reaped), but that is a tidiness win against a correctness loss, so
 * the default is {@code <host>-<pid>}.
 */
public record Options(
    String storeUri,
    String supervisorId,
    Duration interval,
    Duration leaseTtl,
    double capacityWeight,
    Set<TenantId> tenants,
    boolean write,
    Planner.Policy policy,
    Balancer.Config balancer,
    Log.Level logLevel,
    long maxPasses,
    Duration shutdownBudget,
    LivenessTracker.Config liveness
) {

    /** Sentinel for {@code --max-passes}: run until told to stop. */
    public static final long FOREVER = -1L;

    public boolean readOnly() {
        return write == false;
    }

    /** Thrown for a bad argument, and for {@code --help}, which is not an error but is also not a startup. */
    public static final class UsageException extends RuntimeException {
        private final boolean requested;

        UsageException(String message, boolean requested) {
            super(message);
            this.requested = requested;
        }

        /** Whether the user asked for the usage text, as opposed to having it thrown at them. */
        public boolean requested() {
            return requested;
        }
    }

    public static Options parse(String[] args) {
        return parse(args, System.getenv());
    }

    /**
     * Command line first, then environment, then default.
     *
     * <p>Environment fallback because this will run in a container, where {@code SUPERVISOR_STORE} is easier to
     * set than an argv. Command line wins so that a one-off override does not require editing a deployment.
     */
    public static Options parse(String[] args, Map<String, String> env) {
        Map<String, String> given = new LinkedHashMap<>();
        for (String arg : args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                throw new UsageException(usage(), true);
            }
            if (arg.startsWith("--") == false) {
                throw new UsageException("unexpected argument [" + arg + "]; every option starts with --\n\n"
                    + usage(), false);
            }
            int equals = arg.indexOf('=');
            String key = (equals < 0 ? arg.substring(2) : arg.substring(2, equals)).toLowerCase(Locale.ROOT);
            String value = equals < 0 ? "true" : arg.substring(equals + 1);
            if (KNOWN.contains(key) == false) {
                throw new UsageException("unknown option [--" + key + "]\n\n" + usage(), false);
            }
            if (given.put(key, value) != null) {
                // Rather than last-wins, which hides a duplicated line in a deployment template.
                throw new UsageException("option [--" + key + "] was given more than once", false);
            }
        }

        String store = resolve(given, env, "store", null);
        if (store == null || store.isBlank()) {
            throw new UsageException("--store is required\n\n" + usage(), false);
        }

        return new Options(
            store,
            resolve(given, env, "id", defaultId()),
            duration(resolve(given, env, "interval", "2s"), "interval"),
            duration(resolve(given, env, "lease-ttl", "15s"), "lease-ttl"),
            number(resolve(given, env, "capacity-weight", "1.0"), "capacity-weight"),
            tenants(resolve(given, env, "tenants", "")),
            flag(resolve(given, env, "write", "false"), "write"),
            new Planner.Policy(
                integer(resolve(given, env, "work-ceiling", "200"), "work-ceiling"),
                integer(resolve(given, env, "per-tenant-floor", "5"), "per-tenant-floor"),
                integer(resolve(given, env, "minimum-passes", "2"), "minimum-passes"),
                flag(resolve(given, env, "rebalance", "true"), "rebalance")
            ),
            new Balancer.Config(
                number(resolve(given, env, "hysteresis", "0.25"), "hysteresis"),
                integer(resolve(given, env, "release-after-passes", "20"), "release-after-passes"),
                number(resolve(given, env, "default-pool-weight", "1.0"), "default-pool-weight")
            ),
            Log.Level.parse(resolve(given, env, "log-level", "info")),
            integer(resolve(given, env, "max-passes", "-1"), "max-passes"),
            duration(resolve(given, env, "shutdown-budget", "10s"), "shutdown-budget"),
            liveness(given, env)
        );
    }

    private static final Set<String> KNOWN = Set.of(
        "store", "id", "interval", "lease-ttl", "capacity-weight", "tenants", "write", "work-ceiling",
        "per-tenant-floor", "minimum-passes", "rebalance", "hysteresis", "release-after-passes",
        "default-pool-weight", "log-level", "max-passes", "shutdown-budget", "liveness-floor",
        "stale-multiplier", "lost-multiplier"
    );

    /**
     * How patient this supervisor is about a node's silence.
     *
     * <p>Built here rather than inline so the validation lives in one place — {@link LivenessTracker.Config}
     * rejects a stale threshold at or above the lost one, which would remove the stale phase and with it the
     * tolerance for an ordinary GC pause.
     */
    private static LivenessTracker.Config liveness(Map<String, String> given, Map<String, String> env) {
        LivenessTracker.Config defaults = LivenessTracker.Config.defaults();
        try {
            return new LivenessTracker.Config(
                integer(resolve(given, env, "stale-multiplier",
                    Integer.toString(defaults.staleMultiplier())), "stale-multiplier"),
                integer(resolve(given, env, "lost-multiplier",
                    Integer.toString(defaults.lostMultiplier())), "lost-multiplier"),
                duration(resolve(given, env, "liveness-floor",
                    defaults.floor().toMillis() + "ms"), "liveness-floor")
            );
        } catch (IllegalArgumentException rejected) {
            // Config's own message explains what is wrong with the combination; wrapping it as a usage error is
            // what makes it exit 2 with the text on stderr rather than a stack trace.
            throw new UsageException(rejected.getMessage(), false);
        }
    }

    private static String resolve(Map<String, String> given, Map<String, String> env, String key, String fallback) {
        String fromArgs = given.get(key);
        if (fromArgs != null) {
            return fromArgs;
        }
        String fromEnv = env.get("SUPERVISOR_" + key.toUpperCase(Locale.ROOT).replace('-', '_'));
        return fromEnv != null ? fromEnv : fallback;
    }

    /**
     * {@code <host>-<pid>}, which is unique per process rather than stable across restarts. See the class
     * javadoc: uniqueness is a correctness property here and stability is not.
     */
    private static String defaultId() {
        String host = System.getenv("HOSTNAME");
        if (host == null || host.isBlank()) {
            try {
                host = java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                // A machine that cannot resolve its own name is not a reason to refuse to start.
                host = "unknown";
            }
        }
        String shortHost = host.contains(".") ? host.substring(0, host.indexOf('.')) : host;
        return shortHost + "-" + ProcessHandle.current().pid();
    }

    private static Set<TenantId> tenants(String raw) {
        if (raw == null || raw.isBlank()) {
            return Set.of();
        }
        Set<TenantId> tenants = new LinkedHashSet<>();
        for (String part : raw.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty() == false) {
                tenants.add(TenantId.of(trimmed));
            }
        }
        return tenants;
    }

    private static boolean flag(String raw, String name) {
        if (raw.equalsIgnoreCase("true") || raw.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(raw);
        }
        throw new UsageException("--" + name + " takes true or false, not [" + raw + "]", false);
    }

    private static int integer(String raw, String name) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new UsageException("--" + name + " is not a number: [" + raw + "]", false);
        }
    }

    private static double number(String raw, String name) {
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            throw new UsageException("--" + name + " is not a number: [" + raw + "]", false);
        }
    }

    /**
     * {@code 500ms}, {@code 5s}, {@code 2m}, {@code 1h} — and not ISO-8601.
     *
     * <p>{@code Duration.parse} wants {@code PT5S}, which nobody types correctly under pressure.
     */
    static Duration duration(String raw, String name) {
        String value = raw.trim().toLowerCase(Locale.ROOT);
        try {
            if (value.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2).trim()));
            }
            if (value.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1).trim()));
            }
            if (value.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1).trim()));
            }
            if (value.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(value.substring(0, value.length() - 1).trim()));
            }
        } catch (NumberFormatException e) {
            throw new UsageException("--" + name + " is not a duration: [" + raw + "]"
                + "; expected something like 500ms, 5s, 2m or 1h", false);
        }
        throw new UsageException("--" + name + " [" + raw + "] has no unit"
            + "; expected something like 500ms, 5s, 2m or 1h", false);
    }

    /** For the startup line, so that what the process is running with is in the log rather than inferred. */
    public List<String> describe() {
        List<String> pairs = new ArrayList<>();
        pairs.add("store");
        pairs.add(storeUri);
        pairs.add("id");
        pairs.add(supervisorId);
        pairs.add("mode");
        pairs.add(write ? "write" : "read-only");
        pairs.add("interval");
        pairs.add(interval.toMillis() + "ms");
        pairs.add("lease_ttl");
        pairs.add(leaseTtl.toMillis() + "ms");
        pairs.add("capacity_weight");
        pairs.add(Double.toString(capacityWeight));
        pairs.add("tenants");
        pairs.add(tenants.isEmpty() ? "<discovered>" : tenants.toString());
        pairs.add("rebalance");
        pairs.add(Boolean.toString(policy.rebalance()));
        pairs.add("work_ceiling");
        pairs.add(Integer.toString(policy.workCeiling()));
        // The liveness numbers belong in the startup line because they decide when this process starts moving
        // data, which is the single most surprising thing it can do to somebody who did not set them.
        pairs.add("liveness_floor");
        pairs.add(liveness.floor().toMillis() + "ms");
        pairs.add("stale_after");
        pairs.add(liveness.staleMultiplier() + "x");
        pairs.add("lost_after");
        pairs.add(liveness.lostMultiplier() + "x");
        return pairs;
    }

    public static String usage() {
        return """
            opensearch-supervisor — reconciles a pool of OpenSearch nodes against desired state in a key-value store.

            Usage: supervisor --store=<uri> [options]

            Every option may also be given as an environment variable: --lease-ttl is SUPERVISOR_LEASE_TTL.
            The command line wins.

            Required:
              --store=<uri>                Where desired state lives.

            Safety:
              --write                      Actually write assignments. Default is read-only: observe, diff and
                                           project, but never change anything a data node acts on. Start here.
              --id=<string>                This process's identity. Default <host>-<pid>. MUST be unique per
                                           process: two supervisors sharing an id would each believe they hold
                                           the same pool and both write to it.

            Pacing:
              --interval=<duration>        Time between reconciliation passes. Default 2s.
              --lease-ttl=<duration>       Pool lease lifetime. Default 15s. A clean shutdown releases leases,
                                           so this bounds failover only after a crash.
              --max-passes=<n>             Exit after n passes. Default -1, meaning run until stopped. For a
                                           bounded smoke test.
              --shutdown-budget=<duration> How long a clean stop may take before the process stops waiting and
                                           lets the leases expire instead. Default 10s.

            Work limits:
              --work-ceiling=<n>           Shard changes per pass, all tenants. Default 200.
              --per-tenant-floor=<n>       Changes a tenant may have before the ceiling is shared. Default 5.
              --minimum-passes=<n>         Passes a finding must hold before it is acted on. Default 2.
              --rebalance=<true|false>     Whether to move copies to even out load. Default true.

            Fleet:
              --tenants=<a,b,c>            Tenants to manage. Default: discover from the registry.
              --capacity-weight=<double>   This process's share of the fleet's work. Default 1.0.
              --hysteresis=<double>        Fraction over fair share before releasing a pool. Default 0.25.
              --release-after-passes=<n>   Passes over share before releasing. Default 20.
              --default-pool-weight=<d>    Assumed weight of a pool that publishes none. Default 1.0.

            Diagnostics:
              --log-level=<level>          error, warn, info or debug. Default info.
              -h, --help                   This text.

            """ + KeyValueStores.describeAvailable();
    }
}
