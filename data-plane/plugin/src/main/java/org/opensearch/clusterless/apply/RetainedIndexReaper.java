/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.MalformedDocumentException;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.core.index.Index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Decides when an index this node no longer holds may finally be let go of, and its files deleted.
 *
 * <p>{@link DataNodeState} keeps the metadata of an index it holds no copy of, because that is what makes the
 * server release it with {@code NO_LONGER_ASSIGNED} — files kept — rather than {@code DELETED}. Necessary, and it
 * cannot be the end of the story: nothing would ever free the disk, and a node that has outlived a deleted index
 * reports {@code cluster.health} as red with nothing unassigned and a shard percentage of {@code NaN}, because
 * health is being asked about metadata declaring a shard whose routing table has none.
 *
 * <h2>Telling "moved" from "deleted"</h2>
 *
 * <p>An assignment that stops naming a copy does not say which of those happened, and the two want opposite
 * treatment. The index document does say: it is written by the control plane and deleted when the index is, so
 * the node reads {@code tenant/<t>/indices/<name>} for each index it is retaining. It has both halves of that key
 * already, because the tenant and the tenant-scoped name travel in the fabricated settings.
 *
 * <p>The uuid is checked as well as the name, for the reason the resolver checks it: a document present under the
 * same name may describe a <em>different</em> index, and if it does then the one being retained is gone.
 *
 * <h2>Patience, and which way to be wrong</h2>
 *
 * <p>The decision deletes data, so it waits for the same reason the supervisor's {@code ORPHANED_COPY} finding
 * waits: an index document that has not been observed and one that has been deleted look identical, and only one
 * of those readings is recoverable. So the observation has to hold for several consecutive passes, and anything
 * that is not a clear "gone" — a store that cannot be read, a document that will not parse — resets the count
 * rather than advancing it. Waiting costs disk on an index nobody wants; not waiting costs the index.
 *
 * <h2>What this does not survive</h2>
 *
 * <p><b>A restart.</b> The set of retained indices is derived from the previous cluster state, and a new process
 * starts with none — so data left behind by a node that restarted while retaining is never reaped by this, and
 * sits on disk until somebody removes it. That is the dangling-index problem OpenSearch has its own machinery for,
 * and closing it properly means reading the node's data directory rather than its cluster state. Deliberately not
 * attempted here: the in-process case is the one that leaks during normal operation, and a wrong answer while
 * scanning a disk for indices nobody is tracking would delete data with no assignment anywhere to contradict it.
 */
public final class RetainedIndexReaper {

    private static final Logger logger = LogManager.getLogger(RetainedIndexReaper.class);

    private final IndexConfigSource indexConfigs;
    private final int passesBeforeReaping;

    /** How many consecutive passes each retained index has looked gone for. Reset by any doubt. */
    private final Map<Index, Integer> gonePasses = new HashMap<>();

    public RetainedIndexReaper(IndexConfigSource indexConfigs, int passesBeforeReaping) {
        this.indexConfigs = Objects.requireNonNull(indexConfigs, "indexConfigs");
        if (passesBeforeReaping < 1) {
            throw new IllegalArgumentException("passesBeforeReaping must be at least 1, was " + passesBeforeReaping);
        }
        this.passesBeforeReaping = passesBeforeReaping;
    }

    /**
     * Advances the count once, and returns the indices whose metadata may now be dropped.
     *
     * <p><b>Called once per pass, by the poller, and never from the fabrication.</b> That is not a stylistic
     * preference: {@code buildClusterState} runs twice on a pass that applies — once to decide whether anything
     * changed and once inside the applier — so a counter advanced from there would reach its threshold in half the
     * intended time, and would advance at a different rate depending on whether the node happened to be
     * converging. Everything the fabricator is given has to be already decided.
     */
    public Set<Index> reapable(ClusterState applied, DesiredState desired) {
        Set<Index> wanted = new HashSet<>();
        desired.indices()
            .values()
            .forEach(
                index -> wanted.add(
                    new Index(IndexNaming.indexName(index.tenant(), index.uuid(), index.config().name()), index.uuid())
                )
            );

        Set<Index> retained = new LinkedHashSet<>();
        for (IndexMetadata metadata : applied.metadata()) {
            if (wanted.contains(metadata.getIndex()) == false) {
                retained.add(metadata.getIndex());
            }
        }
        // Anything no longer retained -- reassigned back to this node, or already reaped -- forgets its count. A
        // copy that comes and goes must not accumulate strikes across the gaps.
        gonePasses.keySet().retainAll(retained);

        Set<Index> reapable = new LinkedHashSet<>();
        for (IndexMetadata metadata : applied.metadata()) {
            Index index = metadata.getIndex();
            if (retained.contains(index) == false) {
                continue;
            }
            if (stillInTheStore(metadata)) {
                gonePasses.remove(index);
                continue;
            }
            int passes = gonePasses.merge(index, 1, Integer::sum);
            if (passes >= passesBeforeReaping) {
                logger.info(
                    "clusterless: [{}] has been absent from the store for {} passes; releasing its metadata, which "
                        + "lets the server delete what is on disk",
                    index.getName(),
                    passes
                );
                reapable.add(index);
            } else {
                logger.debug(
                    "clusterless: [{}] looks deleted ({} of {} passes); still keeping its data",
                    index.getName(),
                    passes,
                    passesBeforeReaping
                );
            }
        }
        return reapable;
    }

    /**
     * Whether the index document is still there and still describes this index.
     *
     * <p>Errs towards "yes" on every kind of doubt. A store that cannot be read says nothing about whether an
     * index was deleted, and treating silence as deletion is how a network blip becomes data loss.
     */
    private boolean stillInTheStore(IndexMetadata metadata) {
        String tenant = ClusterlessIndexSettings.TENANT.get(metadata.getSettings());
        String name = ClusterlessIndexSettings.TENANT_INDEX_NAME.get(metadata.getSettings());
        if (tenant == null || tenant.isEmpty() || name == null || name.isEmpty()) {
            // Not an index this plugin fabricated, so it is not this plugin's to delete.
            return true;
        }

        try {
            Optional<IndexConfig> config = indexConfigs.read(TenantId.of(tenant), name);
            if (config.isEmpty()) {
                return false;
            }
            // Present under the same name, but possibly a different index -- deleted and recreated. The one being
            // retained is gone either way.
            return config.get().matches(metadata.getIndexUUID());
        } catch (MalformedDocumentException e) {
            logger.debug("clusterless: could not read the index document for [{}]; keeping its data", name, e);
            return true;
        } catch (Exception e) {
            logger.warn("clusterless: could not check whether [{}] still exists; keeping its data", name, e);
            return true;
        }
    }
}
