package org.opensearch.supervisor.app;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;

import java.util.List;

/**
 * Writes {@code pools/definitions/<pool-id>} and exits.
 *
 * <p>A one-shot operator action rather than part of a reconciliation pass, and it exists because of a gap that
 * made the whole system look broken. A pool definition is operator-owned for good reasons — only an operator
 * knows which node attribute names a thing that fails independently, or how many copies a machine should hold —
 * but <b>nothing anywhere could write one</b>. Until one exists there are no pools, so this service runs no
 * reconciliation at all: the control plane accepts an index, data nodes register and heartbeat, and nothing is
 * ever assigned. The only thing that says why is {@code unpooled_tenants} in
 * {@code supervisor/status/housekeeping}, which is not where anybody looks first.
 *
 * <p>Hand-writing the document is not a workaround either. The filesystem backend prefixes each stored value
 * with a revision header, so a file written with an editor comes back as {@code unreadable} rather than as a
 * pool — reported, and easy to misread as a bug in the pool itself.
 *
 * <p><b>Create-if-absent, deliberately.</b> A definition carries an operator's tuning, and overwriting one from
 * a command line that was probably copied out of a README is a worse outcome than refusing. So an existing pool
 * is printed and left alone, and changing it is a deliberate delete-then-define.
 *
 * <p>Not gated behind {@code --write}. That flag exists so a supervisor cannot move data because somebody forgot
 * a flag; this command moves nothing, and typing {@code --define-pool} is itself the intent. Placement follows
 * from a supervisor that is separately allowed to write.
 */
final class DefinePool {

    private static final Log log = Log.of("define-pool");

    /** What {@code --define-pool} and its companions add up to. */
    record Request(
        PoolId pool,
        PoolKind kind,
        boolean isDefault,
        Integer maxShardCopiesPerNode,
        List<String> awarenessAttributes,
        boolean refuseCopiesSharingAHost
    ) {

        PoolDefinition definition() {
            return new PoolDefinition(pool, kind, isDefault, "defined by --define-pool", maxShardCopiesPerNode,
                awarenessAttributes, refuseCopiesSharingAHost);
        }
    }

    private DefinePool() {}

    /**
     * @return the process exit code: 0 written, 1 already there
     */
    static int run(KeyValueStore store, Request request) {
        String key = Keys.poolDefinition(request.pool());

        return store.get(key).map(existing -> {
            log.warn(
                "pool already defined; not touching it",
                "pool",
                request.pool().value(),
                "key",
                key,
                "existing",
                new String(existing.value(), java.nio.charset.StandardCharsets.UTF_8)
            );
            log.warn("delete the key first if you meant to redefine it", "pool", request.pool().value());
            return 1;
        }).orElseGet(() -> {
            PoolDefinition definition = request.definition();
            try {
                // Conditional on absence rather than unconditional, because the read above and this write are
                // two operations: another operator running the same command at the same moment should lose the
                // race visibly rather than silently overwrite.
                store.put(key, definition.toBytes(), KeyValueStore.ABSENT);
            } catch (RevisionConflictException e) {
                log.warn("pool was defined by somebody else while this command ran", "pool", request.pool().value());
                return 1;
            }
            log.info(
                "defined pool",
                "pool",
                request.pool().value(),
                "kind",
                definition.kind(),
                "default",
                definition.isDefault(),
                "awareness_attributes",
                definition.awarenessAttributes(),
                "key",
                key
            );
            if (definition.isDefault()) {
                log.info("a tenant with no pools/bindings entry is served by the default pool, so nothing else needs writing");
            } else {
                log.info(
                    "this pool is not the default, so each tenant it should serve needs a pools/bindings entry",
                    "pool",
                    request.pool().value()
                );
            }
            log.info("nodes that declare this pool become placeable on the next pass of a supervisor started with --write");
            return 0;
        });
    }
}
