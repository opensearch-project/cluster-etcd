package org.opensearch.supervisor.integ;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Where these tests get an etcd.
 *
 * <p>An explicit {@code -Dsupervisor.etcd=host:port} wins; otherwise a container if Docker is available;
 * otherwise the tests skip. One container for the whole JVM, since each test isolates itself in a fresh key
 * prefix.
 *
 * <p>Deliberately a near-copy of {@code EtcdTestServer} in {@code opensearch-kv-store}, rather than something
 * new. That file has four hard-won details in it that are invisible until they bite, and re-deriving them here
 * would mean rediscovering them: etcd needs {@code --listen-client-urls=0.0.0.0} or it binds to loopback inside
 * the container and the mapped port reaches nothing; it refuses to start at all without
 * {@code --advertise-client-urls}; a blank system property must count as unset, because the Gradle test task
 * forwards it unconditionally and an empty string would build the nonsense endpoint {@code ":"}; and the Docker
 * probe must catch {@link Throwable} rather than {@link Exception}, because this backs an {@code @EnabledIf}
 * condition and a condition that <em>throws</em> fails the whole class instead of skipping it — which an
 * incomplete classpath once turned into exactly that, via {@code NoClassDefFoundError}.
 *
 * <p>The duplication is worth naming as a cost: if this pattern needs a third copy, it belongs in the store
 * repository's test fixtures where both builds can share it.
 */
final class EtcdTestServer {

    private static final String ENDPOINT_PROPERTY = "supervisor.etcd";
    private static final String IMAGE_PROPERTY = "supervisor.etcd.image";
    private static final String DEFAULT_IMAGE = "gcr.io/etcd-development/etcd:v3.5.17";
    private static final int CLIENT_PORT = 2379;

    private static final AtomicBoolean REPORTED = new AtomicBoolean();
    private static GenericContainer<?> container;

    private EtcdTestServer() {}

    /** Whether these tests can run at all. Referenced by {@code @EnabledIf}. */
    static boolean available() {
        return configuredEndpoint() != null || dockerAvailable();
    }

    /** {@code host:port}, starting a container on first use if needed. */
    static synchronized String endpoint() {
        String configured = configuredEndpoint();
        if (configured != null) {
            return configured;
        }
        if (container == null) {
            container = new GenericContainer<>(DockerImageName.parse(propertyOr(IMAGE_PROPERTY, DEFAULT_IMAGE)))
                .withExposedPorts(CLIENT_PORT)
                .withCommand(
                    "/usr/local/bin/etcd",
                    "--name=supervisor-integ",
                    "--data-dir=/tmp/etcd",
                    // 0.0.0.0 so the mapped port reaches it; the default binds loopback inside the container.
                    "--listen-client-urls=http://0.0.0.0:" + CLIENT_PORT,
                    // Required even for a single node: etcd will not start without it.
                    "--advertise-client-urls=http://0.0.0.0:" + CLIENT_PORT
                )
                .waitingFor(Wait.forLogMessage(".*ready to serve client requests.*\\n", 1));
            container.start();
        }
        return container.getHost() + ":" + container.getMappedPort(CLIENT_PORT);
    }

    /** A store URI in a key space of its own, so tests cannot see each other's keys. */
    static String storeUri(String namespace) {
        return "etcd://" + endpoint() + "/" + namespace;
    }

    private static String configuredEndpoint() {
        return propertyOr(ENDPOINT_PROPERTY, null);
    }

    /** Blank counts as unset: the Gradle test task forwards these whether or not they were set. */
    private static String propertyOr(String property, String fallback) {
        String configured = System.getProperty(property);
        return configured == null || configured.isBlank() ? fallback : configured;
    }

    private static boolean dockerAvailable() {
        Throwable failure = null;
        boolean usable = false;
        try {
            usable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable noDocker) {
            // Throwable, not Exception: this backs an @EnabledIf, and a condition that throws fails the class
            // rather than skipping it. Anything going wrong while asking is a good enough answer of "no".
            failure = noDocker;
        }
        if (usable == false && REPORTED.compareAndSet(false, true)) {
            String dockerHost = System.getenv("DOCKER_HOST");
            System.err.println("[supervisor-integ] integration tests are being SKIPPED: Testcontainers cannot"
                + " use Docker.");
            System.err.println("  reason:      " + (failure == null
                ? "DockerClientFactory.isDockerAvailable() returned false"
                : failure));
            System.err.println("  DOCKER_HOST: " + describe(dockerHost));
            System.err.println("  socket:      " + describe(System.getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE")));
            // Two failures look identical from here and need opposite responses, so both are named. Which one
            // applies is visible one line up, in Testcontainers' own output: "no valid configuration was found"
            // is the first, "DOCKER_HOST ... is not listening" is the second.
            if (dockerHost == null || dockerHost.isBlank()) {
                System.err.println("  DOCKER_HOST is unset, so Testcontainers looked for a socket at the");
                System.err.println("  conventional /var/run/docker.sock and did not find one. The root build");
                System.err.println("  normally sets DOCKER_HOST from `docker context inspect`, which is how");
                System.err.println("  colima, Rancher Desktop and Apple's `container` are found -- none of them");
                System.err.println("  put the socket where Testcontainers looks by default. If you are running");
                System.err.println("  the tests outside Gradle, export it yourself:");
                System.err.println("    export DOCKER_HOST=$(docker context inspect --format"
                    + " '{{.Endpoints.docker.Host}}')");
            } else {
                System.err.println("  DOCKER_HOST is set, so discovery worked and the daemon is the problem:");
                System.err.println("  the socket is either absent or nothing is listening on it. Check that the");
                System.err.println("  runtime is actually started -- `docker ps` should answer, and for Apple's");
                System.err.println("  container tool that means `container system start` first.");
            }
            System.err.println("  Or bypass containers entirely by pointing at an etcd you already have:");
            System.err.println("    ./gradlew :supervisor-integ:test -D" + ENDPOINT_PROPERTY + "=localhost:2379");
        }
        return usable;
    }

    private static String describe(String value) {
        return value == null || value.isBlank() ? "(unset)" : value;
    }
}
