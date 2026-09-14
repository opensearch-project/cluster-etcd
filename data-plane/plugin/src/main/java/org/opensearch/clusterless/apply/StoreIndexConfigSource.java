/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.Objects;
import java.util.Optional;

/**
 * Index settings and mappings, read from the store one key at a time.
 *
 * <p>One {@code get} per index named in the assignment, rather than a prefix scan of the tenant's indices. An
 * assignment names a handful of indices and a tenant may have thousands, so the scan would read far more than
 * it needs — and it would also be a <b>torn read across keys</b>, which a per-key get is not. That matters
 * here: the uuid check exists precisely to catch a name pointing at the wrong index, and a scan makes the
 * window wider rather than narrower.
 *
 * <p>The revision comes from the store rather than from the document, and travels with the configuration all
 * the way to the shard inventory's {@code config_revision}. It is opaque: this plugin compares it for equality
 * and does nothing else with it.
 */
public final class StoreIndexConfigSource implements IndexConfigSource {

    private final KeyValueStore store;

    public StoreIndexConfigSource(KeyValueStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public Optional<IndexConfig> read(TenantId tenant, String indexName) {
        return store.get(Keys.indexConfig(tenant, indexName)).map(versioned -> IndexConfig.read(versioned.value(), versioned.revision()));
    }
}
