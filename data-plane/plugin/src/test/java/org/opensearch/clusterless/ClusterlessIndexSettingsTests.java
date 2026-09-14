/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless;

import org.junit.jupiter.api.Test;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.settings.SettingsModule;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * That the server will accept the index metadata this plugin intends to fabricate.
 *
 * <p>These assertions exist because the reasoning they replace was wrong. The identity settings were
 * originally left unregistered, on the theory that an index setting OpenSearch does not know is ignored —
 * the tolerant behaviour that lets an index survive being moved to a cluster without some plugin. It is not
 * ignored on the path that matters. {@code IndicesService.createIndexService} calls
 * {@code indexScopedSettings.validate(indexMetadata.getSettings(), true, true, true)} before it builds the
 * {@code IndexModule}, and an unregistered key resolves to a null {@code Setting} and throws.
 *
 * <p>The tolerant behaviour is real and lives elsewhere: {@code MetadataIndexUpgradeService} and
 * {@code ClusterStateUpdaters} call {@code archiveUnknownOrInvalidSettings}, on metadata read from disk
 * during gateway recovery or a version upgrade rather than on a state being applied. Note also what
 * archiving does — it renames the key to {@code archived.*} — so even where it applies, an identity setting
 * would survive as something nothing reads.
 *
 * <p>Nothing else would have caught this until milestone 4, and then as a server-side exception on index
 * creation rather than as anything pointing here. So the check is made against a registry built the way
 * {@code Node} builds it, and the exact call {@code IndicesService} makes.
 */
class ClusterlessIndexSettingsTests {

    /**
     * The registry the node would have, from this plugin's declared settings plus the built-ins.
     *
     * <p>Through {@code SettingsModule} rather than by constructing {@code IndexScopedSettings} directly,
     * because that is the class {@code Node} hands the plugin's list to — including the
     * {@code hasIndexScope()} filter that decides which of them reach the index registry at all.
     */
    private static IndexScopedSettings registryWith(List<Setting<?>> pluginSettings) {
        return new SettingsModule(Settings.EMPTY, pluginSettings, List.of(), java.util.Set.of()).getIndexScopedSettings();
    }

    /** Settings as the fabricator will build them for one index of one tenant. */
    private static Settings fabricatedIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uzKxLWZu4nPSrTknEcDpoA")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(ClusterlessIndexSettings.TENANT.getKey(), "acme")
            .put(ClusterlessIndexSettings.TENANT_INDEX_NAME.getKey(), "my-index")
            .put(ClusterlessIndexSettings.CONFIG_REVISION.getKey(), 3041L)
            .build();
    }

    @Test
    void theServerAcceptsTheIndexSettingsThisPluginWrites() {
        // The exact call IndicesService.createIndexService makes on the apply path: validate dependencies,
        // ignore private settings, ignore archived ones.
        registryWith(new ClusterlessPlugin().getSettings()).validate(fabricatedIndexSettings(), true, true, true);
    }

    @Test
    void anUnregisteredIdentitySettingWouldFailEveryIndexOnTheNode() {
        // The failure the plugin nearly shipped, pinned so the reason survives. Without registration the
        // apply path does not shrug -- it throws, per index, with a message pointing at a missing plugin.
        IndexScopedSettings withoutThem = registryWith(List.of());

        SettingsException e = assertThrows(SettingsException.class, () -> withoutThem.validate(fabricatedIndexSettings(), true, true, true));
        assertTrue(e.getMessage().contains("unknown setting"), e.getMessage());

        // Every one of them fails, not just the one that surfaced. Validation collects an exception per key
        // and rethrows the first with the rest suppressed, and keys iterate in sorted order -- so which one
        // is the message and which are attachments is alphabetical rather than meaningful.
        List<String> reported = new ArrayList<>();
        reported.add(e.getMessage());
        for (Throwable suppressed : e.getSuppressed()) {
            reported.add(suppressed.getMessage());
        }
        for (Setting<?> identity : ClusterlessIndexSettings.all()) {
            assertTrue(
                reported.stream().anyMatch(message -> message.contains(identity.getKey())),
                identity.getKey() + " was not reported as unknown; reported: " + reported
            );
        }
    }

    @Test
    void everyIdentitySettingIsRegisteredByThePlugin() {
        // getSettings() carries both scopes, and the index-scoped half is the half with no other way to be
        // noticed: a missing node setting stops the node booting, a missing index setting waits until
        // something creates an index.
        List<String> declared = new ArrayList<>();
        new ClusterlessPlugin().getSettings().forEach(setting -> declared.add(setting.getKey()));

        for (Setting<?> identity : ClusterlessIndexSettings.all()) {
            assertTrue(declared.contains(identity.getKey()), identity.getKey() + " is not registered by the plugin");
            assertTrue(identity.hasIndexScope(), identity.getKey() + " must be index-scoped to reach the index registry");
            assertTrue(identity.getKey().startsWith(ClusterlessIndexSettings.PREFIX), identity.getKey() + " is outside the reserved prefix");
        }
    }

    @Test
    void theIdentitySettingsAreInternalSoNothingButThisPluginMaySetThem() {
        // What the original "do not register them" instinct was actually reaching for, and the right
        // mechanism for it. InternalIndex is ignored where this plugin needs it to be -- the apply path
        // passes validateInternalOrPrivateIndex: false -- and refused on the update-settings API.
        IndexScopedSettings registry = registryWith(new ClusterlessPlugin().getSettings());

        SettingsException e = assertThrows(
            SettingsException.class,
            () -> registry.validate(
                Settings.builder().put(ClusterlessIndexSettings.TENANT.getKey(), "somebody-elses-tenant").build(),
                true,
                true,
                true,
                true
            )
        );
        assertTrue(e.getMessage().contains("can not update internal setting"), e.getMessage());
    }

    @Test
    void aRevisionSurvivesTheRoundTripThroughSettingsUnchanged() {
        // Settings are strings on the wire, and a revision is opaque. Reading one back as anything other
        // than what was written would silently change which configuration a copy claims to have applied --
        // and the supervisor compares them for equality, so a value that is off by any amount reads as
        // "this node has not caught up" for ever.
        long revision = 9_007_199_254_740_993L; // past 2^53, where a double would round
        Settings settings = Settings.builder().put(ClusterlessIndexSettings.CONFIG_REVISION.getKey(), revision).build();

        assertEquals(revision, ClusterlessIndexSettings.CONFIG_REVISION.get(settings));
    }

    @Test
    void anIndexWithNoRevisionYetReadsAsZeroRatherThanFailing() {
        // Zero is the "nothing applied" value the heartbeat already uses, and a missing setting has to be
        // survivable: torn state is normal, and an index this plugin has not finished fabricating is a
        // pass to wait for rather than an error.
        assertEquals(0L, ClusterlessIndexSettings.CONFIG_REVISION.get(Settings.EMPTY));
    }
}
