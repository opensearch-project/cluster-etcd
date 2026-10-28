package org.opensearch.supervisor.fakenode;

import org.opensearch.controlplane.store.FileSystemKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;

import java.nio.file.Path;
import java.time.Clock;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Runs a {@link FakeNode} against a filesystem store, so that a cluster of them can be watched converging
 * without any of the machinery a real data plane needs.
 *
 * <pre>
 * ../gradlew :fake-node:run --args="--data-path /tmp/sup --node-id node-1 --pool shared-a --zone z1"
 * </pre>
 *
 * <p>The filesystem backend is the only one that works without a network, which is the whole reason this
 * exists in a runnable form: it is how the loop gets exercised end to end in an environment where no socket
 * can be opened.
 */
public final class FakeNodeMain {

    private FakeNodeMain() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        if (options.containsKey("help")) {
            System.out.println("""
                fake-node — a data plane node as far as the store is concerned.

                  --data-path <dir>     filesystem store directory (required)
                  --node-id <id>        persistent node id (required)
                  --pool <id>           pool to declare; omit to be NODE_UNPOOLED
                  --name <name>         node name, defaults to the node id
                  --zone <value>        sets the "zone" awareness attribute
                  --roles data,coordinator
                  --interval-millis <n> heartbeat interval, default 5000
                  --ticks <n>           stop after n ticks; omit to run until interrupted
                """);
            return;
        }

        String dataPath = required(options, "data-path");
        NodeId nodeId = NodeId.of(required(options, "node-id"));
        long intervalMillis = Long.parseLong(options.getOrDefault("interval-millis", "5000"));

        EnumSet<NodeRole> roles = EnumSet.noneOf(NodeRole.class);
        for (String role : options.getOrDefault("roles", "data").split(",")) {
            roles.add(NodeRole.from(role.trim()));
        }
        roles.remove(NodeRole.UNKNOWN);
        if (roles.isEmpty()) {
            throw new IllegalArgumentException("--roles must name at least one of data, coordinator");
        }

        Map<String, String> attributes = new LinkedHashMap<>();
        if (options.containsKey("zone")) {
            attributes.put("zone", options.get("zone"));
        }

        Registration template = new Registration(
            nodeId,
            "assigned-at-start",
            options.getOrDefault("name", nodeId.value()),
            options.containsKey("pool") ? PoolId.of(options.get("pool")) : null,
            roles,
            "127.0.0.1",
            9300,
            "0.1.0-SNAPSHOT",
            attributes,
            intervalMillis,
            0L
        );

        try (KeyValueStore store = new FileSystemKeyValueStore(Path.of(dataPath))) {
            FakeNode node = new FakeNode(store, template, Clock.systemUTC());
            node.start();
            System.out.println("registered " + nodeId + " (ephemeral " + node.ephemeralId() + ")");

            long limit = Long.parseLong(options.getOrDefault("ticks", "-1"));
            for (long tick = 0; limit < 0 || tick < limit; tick++) {
                node.tick();
                System.out.println("tick " + tick + ": holding " + node.inventory().copyCount() + " copies");
                Thread.sleep(intervalMillis);
            }
        }
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") == false) {
                throw new IllegalArgumentException("unexpected argument [" + args[i] + "]; try --help");
            }
            String key = args[i].substring(2);
            if (i + 1 < args.length && args[i + 1].startsWith("--") == false) {
                options.put(key, args[++i]);
            } else {
                options.put(key, "");
            }
        }
        return options;
    }

    private static String required(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("--" + key + " is required; try --help");
        }
        return value;
    }
}
