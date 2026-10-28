package org.opensearch.supervisor.integ;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Where these tests get an etcd.
 *
 * <p>An explicit {@code -Dsupervisor.etcd=host:port} wins; otherwise a container if Docker is available;
 * otherwise the tests skip. One container for the whole JVM, since each test isolates itself in a fresh key
 * prefix.
 *
 * <p>Deliberately a near-copy of {@code EtcdTestServer} in {@code kv-store}, rather than something
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

    static final String ENDPOINT_PROPERTY = "supervisor.etcd";
    private static final String IMAGE_PROPERTY = "supervisor.etcd.image";
    private static final String DEFAULT_IMAGE = "gcr.io/etcd-development/etcd:v3.5.17";
    private static final int CLIENT_PORT = 2379;

    private static GenericContainer<?> container;

    private EtcdTestServer() {}

    /** Whether these tests can run at all. Referenced by {@code @EnabledIf}. */
    static boolean available() {
        return configuredEndpoint() != null || Docker.available(ENDPOINT_PROPERTY);
    }

    /** {@code host:port}, starting a container on first use if needed. */
    static synchronized String endpoint() {
        String configured = configuredEndpoint();
        if (configured != null) {
            return configured;
        }
        if (container == null) {
            container = new GenericContainer<>(DockerImageName.parse(Docker.propertyOr(IMAGE_PROPERTY, DEFAULT_IMAGE)))
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
        return Docker.propertyOr(ENDPOINT_PROPERTY, null);
    }
}
