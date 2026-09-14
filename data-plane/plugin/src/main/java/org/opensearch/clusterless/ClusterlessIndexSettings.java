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
 * <h2>They have to be registered, and this was nearly a bug</h2>
 *
 * <p>The first version of this class said these settings were deliberately <em>not</em> registered with the
 * settings module, on the reasoning that registering them would invite an operator to set them. That
 * reasoning was wrong about how the server treats an index setting it does not know, and nothing caught it
 * because no cluster state is fabricated yet.
 *
 * <p><b>Index settings are strictly validated on the apply path.</b>
 * {@code IndicesService.createIndexService} calls
 * {@code indexScopedSettings.validate(indexMetadata.getSettings(), true, true, true)} before building the
 * {@code IndexModule}, and an unregistered key resolves to a null {@code Setting} and throws
 * {@code SettingsException: unknown setting [index.clusterless.tenant] please check that any required
 * plugins are installed}. So an unregistered identity setting does not mean "ignored", it means every index
 * on the node fails to be created — at milestone 4, in a stack trace pointing at the server rather than
 * here.
 *
 * <p>The tolerant behaviour that makes unknown settings survive a migration is real but lives elsewhere:
 * {@code MetadataIndexUpgradeService} and {@code ClusterStateUpdaters} call
 * {@code archiveUnknownOrInvalidSettings}, which runs on metadata read from disk during gateway recovery or
 * an upgrade, not on a state being applied. And archiving <em>renames</em> the key to {@code archived.*},
 * so even where it applies it would lose the value rather than preserve it.
 *
 * <p>{@code InternalIndex} is what recovers the intent the original reasoning was reaching for, and it does
 * it in two places rather than one. Locally, it is ignored by the apply path — which passes
 * {@code validateInternalOrPrivateIndex: false} — and refused by the update-settings API with "this setting
 * is managed via a dedicated API". Remotely, it is what stops a <em>user</em> supplying one: these settings
 * appear in {@code GET /_cluster/settings_registry}, which walks the node's live registry and emits each
 * property by name, and the control plane maps {@code InternalIndex} onto {@code NOT_USER_SETTABLE} and
 * refuses it on {@code indices.create} and {@code put_settings} alike. So the property is load-bearing on
 * both sides of the store, and {@code ClusterlessIndexSettingsTests} asserts the far end too.
 *
 * <p>Registered so the server will accept them; internal so nothing but this plugin may set them.
 */
public final class ClusterlessIndexSettings {

    private ClusterlessIndexSettings() {}

    /** Which tenant this index belongs to. Index names are only unique within one. */
    public static final Setting<String> TENANT = Setting.simpleString(
        "index.clusterless.tenant",
        Setting.Property.IndexScope,
        Setting.Property.InternalIndex
    );

    /**
     * The index's name <em>in its tenant</em>, which is what the control plane and the supervisor call it
     * and what an operator will search the logs for. Kept separately because the name the server sees has
     * to be unique across tenants and this one does not.
     */
    public static final Setting<String> TENANT_INDEX_NAME = Setting.simpleString(
        "index.clusterless.name",
        Setting.Property.IndexScope,
        Setting.Property.InternalIndex
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
        Setting.Property.IndexScope,
        Setting.Property.InternalIndex
    );

    /** The prefix this plugin reserves. Nothing outside it should carry clusterless identity. */
    public static final String PREFIX = "index.clusterless.";

    public static List<Setting<?>> all() {
        return List.of(TENANT, TENANT_INDEX_NAME, CONFIG_REVISION);
    }
}
