/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane;

import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.handler.OperationContextFactory;
import org.opensearch.controlplane.store.FileSystemKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

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

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = requireValue(args, ++i, "--host");
                case "--port" -> port = Integer.parseInt(requireValue(args, ++i, "--port"));
                case "--data-path" -> dataPath = Path.of(requireValue(args, ++i, "--data-path"));
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
        KeyValueStore store = new FileSystemKeyValueStore(dataPath);
        OperationContextFactory contextFactory = new OperationContextFactory(store, new SingleTenantResolver());
        HandlerRegistry registry = ControlPlaneHandlers.build();

        ControlPlaneServer server = new ControlPlaneServer(host, port, registry, contextFactory);
        if (dumpRoutes) {
            System.out.print(server.dumpRoutes());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "control-plane-shutdown"));

        server.start();
        System.out.printf(
            Locale.ROOT,
            "OpenSearch control plane listening on %s:%d%n%s%nstore: %s%n",
            host,
            server.boundPort(),
            server.describeSurface(),
            store
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
        System.out.println("""
            Usage: control-plane [options]

              --host <addr>       bind address (default 127.0.0.1)
              --port <port>       listen port, 0 for ephemeral (default 9200)
              --data-path <dir>   filesystem store directory (default ./data)
              --dump-routes       print the route table and handler status, then start
              --help              show this message
            """);
    }

}
