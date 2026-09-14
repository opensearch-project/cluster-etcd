/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.Assignment;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.MalformedDocumentException;
import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Every way a copy can be asked for and not built.
 *
 * <p>This class is where the design's tolerance for torn state lives, so these tests are mostly about
 * <em>declining</em>. The property each one protects is the same: a copy that is not built leaves a reason
 * behind. The supervisor will report the shard as unassigned either way, and the difference between a
 * five-minute diagnosis and a long one is whether this node said what it decided and why.
 */
public class DesiredStateResolverTests extends OpenSearchTestCase {

    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private final Map<String, IndexConfig> documents = new HashMap<>();
    private final DesiredStateResolver resolver = new DesiredStateResolver(
        (tenant, name) -> Optional.ofNullable(documents.get(tenant + "/" + name))
    );

    public void testAnAssignedPrimaryIsResolvedAgainstItsIndexDocument() {
        documents.put("acme/my-index", config(UUID, true, 2));

        DesiredState desired = resolver.resolve(assignment(Map.of(0, ShardRole.PRIMARY)));

        assertEquals(1, desired.indices().size());
        assertEquals(1, desired.copyCount());
        assertTrue(desired.deferred().toString(), desired.deferred().isEmpty());
        DesiredState.DesiredIndex index = desired.indices().values().iterator().next();
        assertEquals(ACME, index.tenant());
        assertEquals(UUID, index.uuid());
        assertEquals(ShardRole.PRIMARY, index.shards().get(0));
        assertEquals(3041L, index.config().revision());
    }

    public void testAnIndexDocumentThatHasNotLandedYetDefersRatherThanFails() {
        // Ordinary and temporary. The supervisor may assign a shard before the control plane's index document
        // appears; the two write different keys with no transaction between them.
        DesiredState desired = resolver.resolve(assignment(Map.of(0, ShardRole.PRIMARY)));

        assertTrue(desired.indices().isEmpty());
        assertEquals(1, desired.deferred().size());
        assertTrue(desired.deferred().get(0), desired.deferred().get(0).contains("no index document"));
    }

    public void testAUuidMismatchIsTreatedAsTheIndexNotBeingPresent() {
        // The torn read a later pass would not correct: a name pointing at a different index. Building against
        // it would produce a shard reported under one uuid holding another index's data.
        documents.put("acme/my-index", config("aDifferentUuid1234567", true, 2));

        DesiredState desired = resolver.resolve(assignment(Map.of(0, ShardRole.PRIMARY)));

        assertTrue(desired.indices().isEmpty());
        assertTrue(desired.deferred().get(0), desired.deferred().get(0).contains("not present yet"));
    }

    public void testAClosedIndexHoldsNoCopies() {
        // The contract maps closed to "no copies at all" rather than to a state of its own, matching what the
        // supervisor projects: a closed index is green with no shards.
        documents.put("acme/my-index", config(UUID, false, 2));

        DesiredState desired = resolver.resolve(assignment(Map.of(0, ShardRole.PRIMARY)));

        assertTrue(desired.indices().isEmpty());
        assertTrue(desired.deferred().get(0), desired.deferred().get(0).contains("closed"));
    }

    public void testAnUnreadableIndexDocumentCostsThatIndexAndNothingElse() {
        documents.put("acme/good", config(UUID, true, 1));
        DesiredStateResolver failing = new DesiredStateResolver((tenant, name) -> {
            if (name.equals("broken")) {
                throw new MalformedDocumentException("index configuration", "it is not json");
            }
            return Optional.ofNullable(documents.get(tenant + "/" + name));
        });

        Assignment assignment = new Assignment(
            7,
            0,
            Map.of(
                ACME,
                new Assignment.TenantAssignment(
                    NodeRole.DATA,
                    Map.of(
                        UUID,
                        new Assignment.IndexAssignment("good", 3041, Map.of(0, ShardRole.PRIMARY)),
                        "brokenUuid1234567890",
                        new Assignment.IndexAssignment("broken", 3041, Map.of(0, ShardRole.PRIMARY))
                    )
                )
            )
        );

        DesiredState desired = failing.resolve(assignment);

        assertEquals("the readable index is still resolved", 1, desired.indices().size());
        assertEquals(1, desired.deferred().size());
        assertTrue(desired.deferred().get(0), desired.deferred().get(0).contains("not json"));
    }

    public void testAShardBeyondTheIndexsShardCountIsDeclined() {
        // The assignment and the index document disagree. Building it would fail inside IndexMetadata; declining
        // says which two documents to compare.
        documents.put("acme/my-index", config(UUID, true, 2));

        DesiredState desired = resolver.resolve(assignment(Map.of(0, ShardRole.PRIMARY, 5, ShardRole.PRIMARY)));

        assertEquals(1, desired.copyCount());
        assertTrue(desired.deferred().get(0), desired.deferred().get(0).contains("declares 2 shards"));
    }

    public void testADocumentReplicationReplicaIsDeclinedWithAReasonThatNamesTheGap() {
        // The honest failure. A docrep replica needs to know which node holds its primary, which is peer
        // resolution, which is milestone 9 -- so the copy is not built rather than built as something else.
        documents.put("acme/my-index", config(UUID, true, 2));

        DesiredState desired = resolver.resolve(assignment(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.REPLICA)));

        assertEquals(1, desired.copyCount());
        assertEquals(ShardRole.PRIMARY, desired.indices().values().iterator().next().shards().get(0));
        assertTrue(desired.deferred().get(0), desired.deferred().get(0).contains("peer resolution"));
    }

    public void testASearchReplicaIsBuildable() {
        // Segment replication from the remote store resolves no peers, which is what makes it the role that can
        // exist before peer resolution does.
        documents.put("acme/my-index", config(UUID, true, 2));

        DesiredState desired = resolver.resolve(assignment(Map.of(0, ShardRole.SEARCH_REPLICA)));

        assertEquals(1, desired.copyCount());
        assertTrue(desired.deferred().isEmpty());
    }

    public void testARoleThisBuildDoesNotUnderstandIsDeclinedRatherThanGuessed() {
        // A copy whose role cannot be classified is one that must not be created: there is no way to tell
        // whether the supervisor meant something that takes writes.
        documents.put("acme/my-index", config(UUID, true, 2));

        DesiredState desired = resolver.resolve(assignment(Map.of(0, ShardRole.UNKNOWN)));

        assertTrue(desired.indices().isEmpty());
        assertTrue(desired.deferred().get(0), desired.deferred().get(0).contains("not one this node understands"));
    }

    public void testACoordinatorTenantIsCarriedWithoutReadingAnyIndexDocument() {
        DesiredStateResolver strict = new DesiredStateResolver((tenant, name) -> {
            throw new AssertionError("a coordinator holds no data, so it must not read index documents");
        });

        Assignment assignment = new Assignment(
            7,
            0,
            Map.of(TenantId.of("globex"), new Assignment.TenantAssignment(NodeRole.COORDINATOR, Map.of()))
        );
        DesiredState desired = strict.resolve(assignment);

        assertEquals(List.of(TenantId.of("globex")), desired.coordinatorTenants());
        assertEquals(0, desired.copyCount());
    }

    public void testAnUnknownTenantRoleDoesNothingForThatTenant() {
        Assignment assignment = new Assignment(
            7,
            0,
            Map.of(ACME, new Assignment.TenantAssignment(NodeRole.UNKNOWN, Map.of()))
        );

        DesiredState desired = resolver.resolve(assignment);

        assertTrue(desired.indices().isEmpty());
        assertTrue(desired.coordinatorTenants().isEmpty());
        assertTrue(desired.deferred().get(0), desired.deferred().get(0).contains("not one this node understands"));
    }

    public void testTheReasonsAreBoundedSoAnOutageCannotFillTheLog() {
        // The reasons go onto the heartbeat's 512-character detail and onto a log line. A node whose index
        // documents have all gone missing would otherwise produce one per shard, during exactly the incident
        // when the log has to stay readable.
        Map<Integer, ShardRole> manyShards = new HashMap<>();
        for (int shard = 0; shard < 500; shard++) {
            manyShards.put(shard, ShardRole.PRIMARY);
        }
        documents.put("acme/my-index", config(UUID, true, 1));

        DesiredState desired = resolver.resolve(assignment(manyShards));

        assertEquals(DesiredStateResolver.MAX_REASONS + 1, desired.deferred().size());
        assertTrue(desired.deferred().get(desired.deferred().size() - 1).contains("and more"));
    }

    // ---------------------------------------------------------------- fixtures

    private static IndexConfig config(String uuid, boolean open, int shards) {
        return new IndexConfig(
            "my-index",
            uuid,
            1788969977933L,
            open,
            Map.of("index.number_of_shards", Integer.toString(shards), "index.number_of_replicas", "0"),
            Map.of(),
            3041
        );
    }

    private static Assignment assignment(Map<Integer, ShardRole> shards) {
        return new Assignment(
            7,
            0,
            Map.of(
                ACME,
                new Assignment.TenantAssignment(NodeRole.DATA, Map.of(UUID, new Assignment.IndexAssignment("my-index", 3041, shards)))
            )
        );
    }
}
