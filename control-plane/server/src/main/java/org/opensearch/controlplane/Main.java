/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane;

import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.handler.OperationContextFactory;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.util.Locale;
import java.util.NavigableMap;
import java.util.Optional;

/** Entry point. */
public final class Main {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 9200;

    private Main() {}

    public static void main(String[] args) throws Exception {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        boolean dumpRoutes = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--host" -> host = requireValue(args, ++i, "--host");
                case "--port" -> port = Integer.parseInt(requireValue(args, ++i, "--port"));
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
        //
        // The one missing piece is a durable KeyValueStore. The only implementation is
        // InMemoryKeyValueStore on the test source set, deliberately not shipped, so there is
        // nothing to construct a root store from yet. Add a persistent implementation -- a local
        // filesystem store is the obvious next step -- and this becomes:
        //
        //     KeyValueStore store = new FileSystemKeyValueStore(dataPath);
        //     TenantResolver resolver = new SingleTenantResolver();
        //     var contextFactory = new OperationContextFactory(store, resolver);
        //     var server = new ControlPlaneServer(host, port, ControlPlaneHandlers.build(), contextFactory);
        //
        // Until then every route answers 501 regardless of what is registered.
        System.err.println(
            "warning: no durable KeyValueStore implementation is available, so all operations return 501. "
                + "See the Persistence section of the README."
        );
        HandlerRegistry registry = HandlerRegistry.withAllStubbed();
        OperationContextFactory contextFactory =
            new OperationContextFactory(new UnavailableKeyValueStore(), new SingleTenantResolver());

        ControlPlaneServer server = new ControlPlaneServer(host, port, registry, contextFactory);
        if (dumpRoutes) {
            System.out.print(server.dumpRoutes());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "control-plane-shutdown"));

        server.start();
        System.out.printf(
            Locale.ROOT,
            "OpenSearch control plane listening on %s:%d%n%s%n",
            host,
            server.boundPort(),
            server.describeSurface()
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

              --host <addr>    bind address (default 127.0.0.1)
              --port <port>    listen port, 0 for ephemeral (default 9200)
              --dump-routes    print the generated route table and handler status, then start
              --help           show this message
            """);
    }

    /**
     * Stands in for the durable store that does not exist yet.
     *
     * <p>Unreachable in practice: {@code Main} registers no handlers, so nothing ever asks it for
     * anything. It exists so the server can still start and serve its 501s rather than failing at
     * construction, and it throws rather than silently pretending to persist — a no-op store would
     * make a handler look like it worked.
     */
    private static final class UnavailableKeyValueStore implements KeyValueStore {

        private static UnsupportedOperationException fail() {
            return new UnsupportedOperationException(
                "no durable KeyValueStore is configured; see the Persistence section of the README"
            );
        }

        @Override
        public Optional<Versioned> get(String key) {
            throw fail();
        }

        @Override
        public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            throw fail();
        }

        @Override
        public long put(String key, byte[] value) {
            throw fail();
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            throw fail();
        }

        @Override
        public boolean delete(String key) {
            throw fail();
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            throw fail();
        }
    }
}
