package org.opensearch.supervisor.integ;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Where these tests get a ZooKeeper.
 *
 * <p>Same arrangement as {@link EtcdTestServer}: {@code -Dsupervisor.zookeeper=host:port} wins, otherwise a
 * container if Docker is available, otherwise the tests skip. One container for the whole JVM, since each test
 * isolates itself in a fresh root znode.
 *
 * <p>The one detail specific to this image: it logs {@code binding to port} once the client port is accepting
 * connections, and waiting on the port alone is not enough because ZooKeeper binds before it is ready to serve.
 */
final class ZooKeeperTestServer {

    static final String ENDPOINT_PROPERTY = "supervisor.zookeeper";
    private static final String IMAGE_PROPERTY = "supervisor.zookeeper.image";
    private static final String DEFAULT_IMAGE = "zookeeper:3.9";
    private static final int CLIENT_PORT = 2181;

    private static GenericContainer<?> container;

    private ZooKeeperTestServer() {}

    /** Whether these tests can run at all. Referenced by {@code @EnabledIf}. */
    static boolean available() {
        return configuredEndpoint() != null || Docker.available(ENDPOINT_PROPERTY);
    }

    static synchronized String endpoint() {
        String configured = configuredEndpoint();
        if (configured != null) {
            return configured;
        }
        if (container == null) {
            container = new GenericContainer<>(
                DockerImageName.parse(Docker.propertyOr(IMAGE_PROPERTY, DEFAULT_IMAGE)))
                .withExposedPorts(CLIENT_PORT)
                .waitingFor(Wait.forLogMessage(".*binding to port.*\\n", 1));
            container.start();
        }
        return container.getHost() + ":" + container.getMappedPort(CLIENT_PORT);
    }

    /**
     * A store URI rooted at its own znode.
     *
     * <p>The root is a znode path and is validated as one, which is why {@link StoreIntegBase#freshNamespace()}
     * restricts itself to characters every backend accepts.
     */
    static String storeUri(String namespace) {
        return "zk://" + endpoint() + "/" + namespace;
    }

    private static String configuredEndpoint() {
        return Docker.propertyOr(ENDPOINT_PROPERTY, null);
    }
}
