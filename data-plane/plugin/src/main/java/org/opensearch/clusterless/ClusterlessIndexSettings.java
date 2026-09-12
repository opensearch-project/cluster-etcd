/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * The contract's identity for an index, carried through the fabricated {@link org.opensearch.cluster.ClusterState}.
 *
 * <p>This exists because of a mismatch that has no clean answer. The store is multi-tenant and an index is
 * identified by {@code (tenant, index-uuid)}; a {@code ClusterState} has one flat index namespace keyed by
 * name, and index names are only unique within a tenant. Something has to carry the tenant across, and
 * there are two candidates: encode it into the index name the server sees, or attach it as index metadata.
 *
 * <p><b>Metadata, not the name.</b> Encoding into the name looked obvious and is worse in three ways. Index
 * names are lower-case and exclude several characters that tenant ids and base64 uuids both use, so the
 * encoding would have to be escaped, which makes it unreadable in exactly the logs it would exist to make
 * readable. It would have to be reversible, so the escaping becomes a parser with its own bugs. And the
 * name is not the identity anyway — the uuid is — so keying anything on a name that encodes the uuid is
 * the mistake {@code KEY_LAYOUT.md} warns about, wearing a disguise.
 *
 * <p>Settings avoid all of it. {@link IndexMetadata#SETTING_INDEX_UUID} already exists and takes the
 * control plane's uuid unchanged, which means {@link org.opensearch.core.index.Index} equality — used
 * throughout the server — is already identity in the contract's sense. The other two settings carry what
 * the uuid alone cannot say, and the index name the server sees is then free to be whatever reads best.
 *
 * <p>None of these are registered with the settings module: they are index metadata written into a
 * fabricated cluster state by this plugin and read back by it, never set by an operator. Registering them
 * would invite exactly that.
 */
public final class ClusterlessIndexSettings {

    private ClusterlessIndexSettings() {}

    /** Which tenant this index belongs to. Index names are only unique within one. */
    public static final Setting<String> TENANT = Setting.simpleString("index.clusterless.tenant", Setting.Property.IndexScope);

    /**
     * The index's name <em>in its tenant</em>, which is what the control plane and the supervisor call it
     * and what an operator will search the logs for. Kept separately because the name the server sees has
     * to be unique across tenants and this one does not.
     */
    public static final Setting<String> TENANT_INDEX_NAME = Setting.simpleString(
        "index.clusterless.name",
        Setting.Property.IndexScope
    );

    /**
     * The store revision of the {@code tenant/<t>/indices/<name>} document this index was built from, and
     * what the inventory reports as {@code config_revision} so the supervisor can tell whether this node
     * has caught up with a settings change.
     *
     * <p>The SPI types a revision as a {@code long} and the contract writes it as a number, so this is a
     * long. That is a representation and not a licence: revisions are <b>opaque and comparable only for
     * equality</b>. No ordering, no arithmetic, and no assuming they advance by one — on etcd this is a
     * cluster-wide counter that moves when unrelated keys change, and on ZooKeeper it is a zxid.
     *
     * <p>The minimum is unbounded for the same reason. Bounding it at zero would be assuming something
     * about a value a backend is free to define however it likes.
     */
    public static final Setting<Long> CONFIG_REVISION = Setting.longSetting(
        "index.clusterless.config_revision",
        0L,
        Long.MIN_VALUE,
        Setting.Property.IndexScope
    );

    public static List<Setting<?>> all() {
        return List.of(TENANT, TENANT_INDEX_NAME, CONFIG_REVISION);
    }
}
