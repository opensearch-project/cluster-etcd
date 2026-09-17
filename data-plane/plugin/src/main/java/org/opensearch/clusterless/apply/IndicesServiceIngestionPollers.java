/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.clusterless.contract.IngestionReset;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.pollingingest.IngestionSettings;
import org.opensearch.indices.pollingingest.StreamPoller;

import java.util.function.Supplier;

/**
 * Moving a real consumer, which is the one part of a reset that cannot be a pure function.
 *
 * <p><b>Two calls, and the order is the whole point.</b> {@code IngestionEngine.resetStreamPoller} throws
 * {@code IllegalStateException("Cannot reset consumer when poller is not paused")}, and
 * {@code IngestionEngine.updateIngestionSettings} applies a reset <em>before</em> it applies the settings' pause
 * flag. So a single call carrying both the reset and the pause state cannot work: the reset runs first, against
 * whatever the poller was doing. The pause has to be a call of its own, completed before the reset is asked for.
 *
 * <p>The reason this cannot be skipped when the index document says the stream should be running is in
 * {@link IngestionResets}: the resume reaches this node several passes before the reset does, because one is
 * desired state and the other is an instruction. By the time a reset arrives, the ordinary state of the poller is
 * <em>running</em>.
 *
 * <p>The second call carries {@code setIsPaused} as well as the reset, which is safe in the other direction —
 * upstream applies the reset first and the flag second within one call, which is exactly the order wanted — and it
 * means the stream is stopped for one reset rather than for a whole extra pass.
 *
 * <p>Nothing here reports success by reading the consumer back, and that is deliberate: a shard reset to offset
 * 1000 immediately starts advancing, so a position read afterwards cannot distinguish a reset that worked from one
 * that never happened. The acknowledgement is that this returned without throwing.
 */
public final class IndicesServiceIngestionPollers implements IngestionResets.Pollers {

    private static final Logger logger = LogManager.getLogger(IndicesServiceIngestionPollers.class);

    private final Supplier<IndicesService> indicesService;

    public IndicesServiceIngestionPollers(Supplier<IndicesService> indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public Readiness readinessOf(ShardId shard) {
        IndexShard indexShard = shardOrNull(shard);
        if (indexShard == null || indexShard.state() != org.opensearch.index.shard.IndexShardState.STARTED) {
            return Readiness.NOT_YET;
        }
        if (indexShard.indexSettings().getIndexMetadata().useIngestionSource() == false) {
            return Readiness.NOT_INGESTING;
        }
        if (IngestionResets.isResettableCopy(indexShard.routingEntry()) == false) {
            // The routing entry, not the assignment: this is what the server believes, and a copy that is not a
            // started primary here has no consumer of its own whatever the assignment says.
            return Readiness.NOT_YET;
        }
        return Readiness.READY;
    }

    @Override
    public void pauseThenReset(ShardId shard, IngestionReset.Mode mode, String value, boolean landPaused) throws Exception {
        IndexShard indexShard = shardOrNull(shard);
        if (indexShard == null) {
            throw new IllegalStateException("shard " + shard + " is no longer here");
        }

        // First call: stop the poller, whatever it was doing. Idempotent on an already-paused one.
        indexShard.updateShardIngestionState(IngestionSettings.builder().setIsPaused(true).build());

        // Second: move the consumer, then land on the state the index document actually asks for. Upstream
        // applies these in this order within the one call, which is the order wanted.
        indexShard.updateShardIngestionState(
            IngestionSettings.builder().setResetState(resetStateOf(mode)).setResetValue(value).setIsPaused(landPaused).build()
        );

        logger.info(
            "clusterless: moved {}'s consumer to {} [{}], and left ingestion {}",
            shard,
            mode.name().toLowerCase(java.util.Locale.ROOT),
            value,
            landPaused ? "paused" : "running"
        );
    }

    /**
     * @throws IllegalArgumentException for {@code UNKNOWN}, which must never reach here — {@code IngestionResets}
     *     refuses it with a reason instead, because a mode this node cannot map is one the supervisor has to be
     *     told about rather than one to pick a default for
     */
    private static StreamPoller.ResetState resetStateOf(IngestionReset.Mode mode) {
        return switch (mode) {
            case OFFSET -> StreamPoller.ResetState.RESET_BY_OFFSET;
            case TIMESTAMP -> StreamPoller.ResetState.RESET_BY_TIMESTAMP;
            case UNKNOWN -> throw new IllegalArgumentException("an unknown reset mode must be refused, not applied");
        };
    }

    private IndexShard shardOrNull(ShardId shard) {
        IndicesService indices = indicesService.get();
        if (indices == null) {
            return null;
        }
        try {
            IndexService indexService = indices.indexService(shard.getIndex());
            return indexService == null ? null : indexService.getShardOrNull(shard.id());
        } catch (Exception e) {
            logger.debug("clusterless: could not reach {}", shard, e);
            return null;
        }
    }
}
