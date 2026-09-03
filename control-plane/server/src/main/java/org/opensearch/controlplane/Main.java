/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane;

import org.opensearch.controlplane.capability.DataPlaneCapabilities;
import org.opensearch.controlplane.capability.DeclaredCapabilities;
import org.opensearch.controlplane.capability.DeclaredSettings;
import org.opensearch.controlplane.capability.PermissiveCapabilities;
import org.opensearch.controlplane.capability.PermissiveSettings;
import org.opensearch.controlplane.capability.SettingsRegistry;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.handler.OperationContextFactory;
import org.opensearch.controlplane.store.FileSystemKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.net.BindException;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.Locale;

/** Entry point. */
public final class Main {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9200;
    private static final String DEFAULT_DATA_PATH = "./data";

    private Main() {}

    public static void main(String[] args) throws Exception {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        boolean dumpRoutes = false;
        Path dataPath = Path.of(DEFAULT_DATA_PATH);
        ClusterIdentity identity = ClusterIdentity.defaults();
        Path capabilitiesPath = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = requireValue(args, ++i, "--host");
                case "--port" -> port = Integer.parseInt(requireValue(args, ++i, "--port"));
                case "--data-path" -> dataPath = Path.of(requireValue(args, ++i, "--data-path"));
                case "--cluster-name" -> identity = identity.withClusterName(requireValue(args, ++i, "--cluster-name"));
                case "--capabilities" -> capabilitiesPath = Path.of(requireValue(args, ++i, "--capabilities"));
                case "--dump-routes" -> dumpRoutes = true;
                case "--help", "-h" -> {
                    printUsage();
                    return;
                }
                default -> {
                    System.err.println("unknown option: " + args[i]);
                    printUsage();
                    System.exit(2);
                    return;
                }
            }
        }

        // Wiring lives here so the routing and HTTP layers stay unaware of storage and tenancy.
        // Swapping SingleTenantResolver for a principal- or host-derived resolver is the only change
        // needed to serve multiple tenants; see the README's Multi-tenancy section.
        DataPlaneCapabilities capabilities;
        SettingsRegistry settings;
        if (capabilitiesPath == null) {
            // Said out loud rather than left implicit: without this, a pipeline naming a processor the
            // data plane does not have is accepted and only fails when it runs, and an unknown setting
            // is stored rather than rejected.
            System.err.println(
                "warning: no --capabilities file, so processor types and settings are not validated. "
                    + "Generate one with tools/dump-capabilities.sh <data-plane-url>."
            );
            capabilities = new PermissiveCapabilities();
            settings = new PermissiveSettings();
        } else {
            capabilities = DeclaredCapabilities.load(capabilitiesPath);
            // The settings section is optional, so a capabilities file generated before the data plane
            // exposed its settings registry still loads and simply does not enforce settings.
            settings = DeclaredSettings.load(capabilitiesPath)
                .map(declared -> (SettingsRegistry) declared)
                .orElseGet(() -> {
                    System.err.println(
                        "warning: --capabilities file has no `settings` section, so settings are not "
                            + "validated. Regenerate it against a data plane that exposes the registry."
                    );
                    return new PermissiveSettings();
                });
        }

        KeyValueStore store = new FileSystemKeyValueStore(dataPath);
        OperationContextFactory contextFactory = new OperationContextFactory(store, new SingleTenantResolver());
        HandlerRegistry registry = ControlPlaneHandlers.build(identity, capabilities);

        ControlPlaneServer server = new ControlPlaneServer(host, port, registry, contextFactory);
        if (dumpRoutes) {
            System.out.print(server.dumpRoutes());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "control-plane-shutdown"));

        try {
            server.start();
        } catch (Exception e) {
            // Caught broadly and then narrowed by hand, rather than with specific catch clauses.
            // Netty's bind future rethrows the original failure via an unchecked cast, so the
            // compiler does not believe BindException or SocketException can be thrown here and
            // rejects a catch clause for either. Same reason NettyHttpServer.start catches Throwable.
            if (e instanceof BindException) {
                // Overwhelmingly the common case in development: a real OpenSearch already holds 9200.
                System.err.printf(
                    Locale.ROOT,
                    "error: cannot bind %s:%d (%s)%n%n"
                        + "Something is already listening there. During development that is usually a real%n"
                        + "OpenSearch; pick another port and you can run both side by side:%n%n"
                        + "  --port 9400%n",
                    host,
                    port,
                    e.getMessage()
                );
            } else if (e instanceof SocketException) {
                System.err.printf(Locale.ROOT, "error: cannot bind %s:%d (%s)%n", host, port, e.getMessage());
            } else {
                throw e;
            }
            System.exit(1);
            return;
        }

        System.out.printf(
            Locale.ROOT,
            "OpenSearch control plane listening on %s:%d%n%s%nstore: %s%ncapabilities: %s%nsettings: %s%n",
            host,
            server.boundPort(),
            server.describeSurface(),
            store,
            capabilities,
            settings
        );
        server.awaitShutdown();
    }

    private static String requireValue(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException(option + " requires a value");
        }
        return args[index];
    }

    private static void printUsage() {
        System.out.printf(
            Locale.ROOT,
            """
            Usage: control-plane [options]

              --host <addr>       bind address (default %s)
              --port <port>       listen port, 0 for ephemeral (default %d)
              --data-path <dir>   filesystem store directory (default %s)
              --cluster-name <n>  cluster_name reported by GET / (default opensearch)
              --capabilities <f>  data plane capabilities file; without it, processor types
                                  and settings are not validated. Generate with
                                  tools/dump-capabilities.sh
              --dump-routes       print the route table and handler status, then start
              --help              show this message

            Reports itself as OpenSearch %s. That is a build-time property, set by
            target_opensearch_version in spec/control-plane-scope.yaml, because it also decides
            which operations the route table contains.
            %n""",
            DEFAULT_HOST,
            DEFAULT_PORT,
            DEFAULT_DATA_PATH,
            ClusterIdentity.DEFAULT_VERSION
        );
    }
}
