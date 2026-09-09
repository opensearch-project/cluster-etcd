/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.zookeeper;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Where the ZooKeeper integration tests get an ensemble.
 *
 * <p>Two sources, in this order:
 *
 * <ol>
 *   <li><b>{@code -Dcontrolplane.zookeeper=host:port}</b> — an ensemble you are already running. Wins when
 *       set, because starting a container takes seconds and a developer iterating on one test should not pay
 *       that, and because pointing the suite at a specific build is sometimes the point.</li>
 *   <li><b>A container</b>, if Docker is available. This is what CI uses: no configuration, just Docker.</li>
 * </ol>
 *
 * <p>With neither, the tests skip. That is deliberate rather than a failure: this repository is also worked
 * on in an environment with no Docker and no outbound sockets at all, where the encoding tests still run and
 * still carry most of the risk.
 *
 * <h2>One container for the whole JVM</h2>
 *
 * <p>Started once, lazily, and never stopped — the standard Testcontainers singleton pattern. Ryuk removes
 * it when the JVM exits. Starting one per test would multiply a several-second cost by thirty, and it is
 * unnecessary because every test already isolates itself in a fresh root znode.
 */
final class ZooKeeperTestServer {

    /** Overridable, because the image a deployment cares about is not always the newest. */
    private static final String IMAGE =
        imageOr("controlplane.zookeeper.image", "zookeeper:3.9");

    private static final String ENDPOINT_PROPERTY = "controlplane.zookeeper";
    private static final int CLIENT_PORT = 2181;

    private static GenericContainer<?> container;

    private ZooKeeperTestServer() {}

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
                // The image logs this once the client port is accepting connections. Waiting on the port
                // alone is not enough: ZooKeeper binds before it is ready to serve.
                .waitingFor(Wait.forLogMessage(".*binding to port.*\\n", 1));
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
