/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.etcd;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Where the etcd integration tests get a cluster.
 *
 * <p>Same arrangement as {@code ZooKeeperTestServer}: an explicit
 * {@code -Dcontrolplane.etcd=host:port} wins, otherwise a container if Docker is available, otherwise the
 * tests skip. One container for the whole JVM, since each test isolates itself in a fresh key prefix.
 *
 * <h2>etcd needs telling to listen</h2>
 *
 * <p>Unlike the ZooKeeper image, the etcd image will not serve anything useful unless it is told to.
 * {@code --listen-client-urls} has to be {@code 0.0.0.0} or it binds to loopback inside the container and
 * nothing outside can reach it, and {@code --advertise-client-urls} has to be set or etcd refuses to start.
 * Single-node clusters also need authentication explicitly disabled on some images.
 */
final class EtcdTestServer {

    /**
     * Overridable, because a registry that is reachable in one network is not in another, and because the
     * version a deployment runs is not always the newest. {@code quay.io/coreos/etcd} and
     * {@code gcr.io/etcd-development/etcd} both publish the same builds.
     */
    private static final String IMAGE =
        imageOr("controlplane.etcd.image", "gcr.io/etcd-development/etcd:v3.5.17");

    private static final String ENDPOINT_PROPERTY = "controlplane.etcd";
    private static final int CLIENT_PORT = 2379;

    private static GenericContainer<?> container;

    private EtcdTestServer() {}

    /** Whether the integration tests can run at all. Referenced by {@code @EnabledIf}. */
    static boolean available() {
        return configuredEndpoint() != null || dockerAvailable();
    }

    /**
     * The explicitly configured endpoint, or null.
     *
     * <p>Blank counts as absent, which matters more than it looks: the Gradle {@code test} task forwards
     * this property unconditionally and passes an empty string when it was not set, so a plain null check
     * would read "" as a configured endpoint and build the nonsense URI {@code SCHEME://}. The previous
     * {@code @EnabledIfSystemProperty(matches = ".+")} absorbed that silently.
     */
    private static String configuredEndpoint() {
        String configured = System.getProperty(ENDPOINT_PROPERTY);
        return configured == null || configured.isBlank() ? null : configured;
    }

    /** {@code host:port} to connect to, starting a container on first use if needed. */
    static synchronized String endpoint() {
        String configured = configuredEndpoint();
        if (configured != null) {
            return configured;
        }
        if (container == null) {
            container = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withExposedPorts(CLIENT_PORT)
                .withCommand(
                    "/usr/local/bin/etcd",
                    "--name=controlplane-test",
                    "--data-dir=/tmp/etcd",
                    // 0.0.0.0 so the mapped port reaches it; without this etcd listens on loopback only.
                    "--listen-client-urls=http://0.0.0.0:" + CLIENT_PORT,
                    // Required even for a single node: etcd will not start without it.
                    "--advertise-client-urls=http://0.0.0.0:" + CLIENT_PORT
                )
                .waitingFor(Wait.forLogMessage(".*ready to serve client requests.*\\n", 1));
            container.start();
        }
        return container.getHost() + ":" + container.getMappedPort(CLIENT_PORT);
    }

    /** Blank counts as unset here too, for the same reason. */
    private static String imageOr(String property, String fallback) {
        String configured = System.getProperty(property);
        return configured == null || configured.isBlank() ? fallback : configured;
    }

    /**
     * Whether Docker can be used, answering false rather than propagating anything.
     *
     * <p>{@link Throwable} on purpose, which is worth justifying because catching {@link Error} is normally
     * wrong. This method backs an {@code @EnabledIf} condition, and a condition that throws does not disable
     * the tests — it <b>fails the whole class</b>, which is the opposite of what "cannot run here" should
     * mean. Anything at all going wrong while asking the question is a good enough answer of "no".
     *
     * <p>Not hypothetical: an incomplete classpath produced {@code NoClassDefFoundError} here, an
     * {@link Error} rather than an exception, and turned a skip into a failure.
     */
    private static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable noDocker) {
            return false;
        }
    }
}
