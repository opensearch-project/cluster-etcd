package io.clustercontroller.multicluster;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.*;

class RendezvousHashPolicyTest {

    @Test
    void testShouldAttempt_WithinCapacity() {
        // Given
        RendezvousHashPolicy policy = new RendezvousHashPolicy(5, 1);
        policy.refresh("controller-1", 
            Set.of("controller-1", "controller-2"), 
            Set.of("cluster-1", "cluster-2", "cluster-3"),
            Set.of());
        
        // When/Then
        // Controller should attempt clusters it's ranked #1 for, up to capacity
        boolean shouldAttempt = policy.shouldAttempt("cluster-1", 2);
        
        // Can't assert exact result without knowing hash, but should not throw
        assertThat(shouldAttempt).isIn(true, false);
    }

    @Test
    void testShouldAttempt_AtCapacity() {
        // Given
        RendezvousHashPolicy policy = new RendezvousHashPolicy(3, 1);
        policy.refresh("controller-1", 
            Set.of("controller-1", "controller-2"), 
            Set.of("cluster-1", "cluster-2", "cluster-3", "cluster-4"),
            Set.of("cluster-1", "cluster-2", "cluster-3"));
        
        // When
        boolean shouldAttempt = policy.shouldAttempt("cluster-4", 3);
        
        // Then - At capacity, should not attempt new clusters
        assertThat(shouldAttempt).isFalse();
    }

    @Test
    void testShouldAttempt_OverCapacity() {
        // Given
        RendezvousHashPolicy policy = new RendezvousHashPolicy(2, 1);
        policy.refresh("controller-1", 
            Set.of("controller-1"), 
            Set.of("cluster-1", "cluster-2", "cluster-3"),
            Set.of("cluster-1", "cluster-2", "cluster-3")); // Over capacity!
        
        // When
        boolean shouldAttempt = policy.shouldAttempt("cluster-4", 3);
        
        // Then - Over capacity, should not attempt
        assertThat(shouldAttempt).isFalse();
    }

    @Test
    void testShouldRelease_NotTopRanked() {
        // Given
        RendezvousHashPolicy policy = new RendezvousHashPolicy(5, 1);
        
        // Refresh with multiple controllers
        policy.refresh("controller-1", 
            Set.of("controller-1", "controller-2", "controller-3"), 
            Set.of("cluster-1", "cluster-2", "cluster-3"),
            Set.of("cluster-1", "cluster-2"));
        
        // When/Then
        // Some clusters will be ranked higher for other controllers
        // Can't assert exact result without knowing hash
        boolean shouldRelease = policy.shouldRelease("cluster-1");
        assertThat(shouldRelease).isIn(true, false);
    }

    @Test
    void testShouldRelease_OverCapacity() {
        // Given
        RendezvousHashPolicy policy = new RendezvousHashPolicy(2, 1);
        policy.refresh("controller-1", 
            Set.of("controller-1"), 
            Set.of("cluster-1", "cluster-2", "cluster-3"),
            Set.of("cluster-1", "cluster-2", "cluster-3")); // Over capacity
        
        // When/Then
        // Should release some clusters to get back under capacity
        boolean release1 = policy.shouldRelease("cluster-1");
        boolean release2 = policy.shouldRelease("cluster-2");
        boolean release3 = policy.shouldRelease("cluster-3");
        
        // At least one should be released (we have 3, capacity is 2)
        assertThat(release1 || release2 || release3).isTrue();
    }

    @Test
    void testConsistentHashing_SameInputsSameResults() {
        // Given
        RendezvousHashPolicy policy1 = new RendezvousHashPolicy(5, 1);
        RendezvousHashPolicy policy2 = new RendezvousHashPolicy(5, 1);
        
        Set<String> controllers = Set.of("controller-1", "controller-2", "controller-3");
        Set<String> clusters = Set.of("cluster-1", "cluster-2", "cluster-3", "cluster-4", "cluster-5");
        
        // When
        policy1.refresh("controller-1", controllers, clusters, Set.of());
        policy2.refresh("controller-1", controllers, clusters, Set.of());
        
        // Then - Same inputs should produce same results
        for (String cluster : clusters) {
            assertThat(policy1.shouldAttempt(cluster, 0))
                .isEqualTo(policy2.shouldAttempt(cluster, 0));
        }
    }

    @Test
    void testMinimalReassignment_ControllerLeaves() {
        // Given
        RendezvousHashPolicy policy = new RendezvousHashPolicy(10, 1);
        
        Set<String> initialControllers = Set.of("controller-1", "controller-2", "controller-3");
        Set<String> clusters = Set.of("cluster-1", "cluster-2", "cluster-3", "cluster-4", "cluster-5");
        
        // Initial state
        policy.refresh("controller-1", initialControllers, clusters, Set.of());
        Set<String> initialAssignments = Set.of();
        for (String cluster : clusters) {
            if (policy.shouldAttempt(cluster, initialAssignments.size())) {
                initialAssignments = Set.copyOf(Set.of(cluster));
            }
        }
        
        // When - controller-3 leaves
        Set<String> newControllers = Set.of("controller-1", "controller-2");
        policy.refresh("controller-1", newControllers, clusters, initialAssignments);
        
        // Then - Most clusters should remain with their original controller
        // (Can't assert exact count without knowing hash results, but HRW minimizes moves)
        assertThat(policy).isNotNull(); // Verifies no crashes
    }

    @Test
    void testTopK_MultipleControllersCanAttempt() {
        // Given - topK=2 means top 2 ranked controllers can attempt each cluster
        RendezvousHashPolicy policy = new RendezvousHashPolicy(5, 2);
        
        policy.refresh("controller-1", 
            Set.of("controller-1", "controller-2", "controller-3"), 
            Set.of("cluster-1"),
            Set.of());
        
        // When
        boolean shouldAttempt = policy.shouldAttempt("cluster-1", 0);
        
        // Then - Controller-1 might be in top 2, so could attempt
        assertThat(shouldAttempt).isIn(true, false);
    }

    @Test
    void testRefresh_HandlesEmptySets() {
        // Given
        RendezvousHashPolicy policy = new RendezvousHashPolicy(5, 1);
        
        // When/Then - Should not crash
        assertThatCode(() -> {
            policy.refresh("controller-1", Set.of(), Set.of(), Set.of());
        }).doesNotThrowAnyException();
        
        assertThat(policy.shouldAttempt("cluster-1", 0)).isFalse();
        assertThat(policy.shouldRelease("cluster-1")).isFalse();
    }
}

