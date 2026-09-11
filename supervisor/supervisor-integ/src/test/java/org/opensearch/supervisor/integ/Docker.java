package org.opensearch.supervisor.integ;

import org.testcontainers.DockerClientFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Whether Testcontainers can use Docker, and — when it cannot — why.
 *
 * <p>Shared by every backend's test server rather than copied per backend, because the interesting content is the
 * diagnosis and there is no version of "explain this twice" that stays in step.
 */
final class Docker {

    private static final AtomicBoolean REPORTED = new AtomicBoolean();

    private Docker() {}

    /**
     * Answers false rather than propagating anything, and <b>says why</b>.
     *
     * <p>{@link Throwable} on purpose, which is worth justifying because catching {@link Error} is normally
     * wrong. This backs an {@code @EnabledIf} condition, and a condition that <em>throws</em> does not disable the
     * tests — it fails the whole class, which is the opposite of what "cannot run here" should mean. Anything
     * going wrong while asking the question is a good enough answer of no. Not hypothetical: an incomplete
     * classpath produced {@code NoClassDefFoundError} here, an {@link Error} rather than an exception, and turned
     * a skip into a failure.
     *
     * <p>The reason is printed because swallowing it made a worse bug than the one it fixed. Somebody running
     * Docker who sees the tests skip anyway has nothing to go on, and {@code @EnabledIf} cannot carry a computed
     * message into its {@code disabledReason}. So the explanation goes to stderr, once per JVM.
     *
     * @param endpointProperty the {@code -D} property that bypasses containers, named in the advice
     */
    static boolean available(String endpointProperty) {
        Throwable failure = null;
        boolean usable = false;
        try {
            usable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable noDocker) {
            failure = noDocker;
        }
        if (usable == false && REPORTED.compareAndSet(false, true)) {
            report(endpointProperty, failure);
        }
        return usable;
    }

    private static void report(String endpointProperty, Throwable failure) {
        String dockerHost = System.getenv("DOCKER_HOST");
        System.err.println("[supervisor-integ] integration tests are being SKIPPED: Testcontainers cannot use"
            + " Docker.");
        System.err.println("  reason:      " + (failure == null
            ? "DockerClientFactory.isDockerAvailable() returned false"
            : failure));
        System.err.println("  DOCKER_HOST: " + describe(dockerHost));
        System.err.println("  socket:      " + describe(System.getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE")));
        // Two failures look identical from here and need opposite responses, so both are named. Which one
        // applies is visible one line up, in Testcontainers' own output: "no valid configuration was found" is
        // the first, "DOCKER_HOST ... is not listening" is the second.
        if (dockerHost == null || dockerHost.isBlank()) {
            System.err.println("  DOCKER_HOST is unset, so Testcontainers looked for a socket at the");
            System.err.println("  conventional /var/run/docker.sock and did not find one. The root build");
            System.err.println("  normally sets DOCKER_HOST from `docker context inspect`, which is how");
            System.err.println("  Docker-compatible runtimes are found -- several do not put the socket");
            System.err.println("  where Testcontainers looks by default. If you are running the tests");
            System.err.println("  outside Gradle, export it yourself:");
            System.err.println("    export DOCKER_HOST=$(docker context inspect --format"
                + " '{{.Endpoints.docker.Host}}')");
        } else {
            System.err.println("  DOCKER_HOST is set, so discovery worked and the daemon is the problem:");
            System.err.println("  the socket is either absent, or nothing is listening on it, or the daemon");
            System.err.println("  is reachable but a container could not be started. `docker ps` should");
            System.err.println("  answer; some runtimes need starting with their own command first.");
        }
        System.err.println("  Or bypass containers entirely by pointing at a server you already have:");
        System.err.println("    ./gradlew :supervisor-integ:test -D" + endpointProperty + "=host:port");
    }

    private static String describe(String value) {
        return value == null || value.isBlank() ? "(unset)" : value;
    }

    /** Blank counts as unset: the Gradle test task forwards these whether or not they were set. */
    static String propertyOr(String property, String fallback) {
        String configured = System.getProperty(property);
        return configured == null || configured.isBlank() ? fallback : configured;
    }
}
