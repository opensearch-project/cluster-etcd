package io.clustercontroller.allocation;

import io.clustercontroller.allocation.deciders.*;
import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AllocationDecisionEngineTest {
    
    private AllocationDecisionEngine engine;
    private SearchUnit healthyPrimary;
    private SearchUnit unhealthyNode;
    private SearchUnit coordinatorNode;
    
    @BeforeEach
    void setUp() {
        engine = new AllocationDecisionEngine();
        
        // Register deciders
        engine.registerDecider(AllNodesDecider.class, new AllNodesDecider());
        engine.registerDecider(HealthDecider.class, new HealthDecider());
        engine.registerDecider(RoleDecider.class, new RoleDecider());
        engine.registerDecider(ShardPoolDecider.class, new ShardPoolDecider());
        
        // Create test nodes
        healthyPrimary = createSearchUnit("node1", "PRIMARY", "NORMAL", HealthState.GREEN, "0");
        unhealthyNode = createSearchUnit("node2", "PRIMARY", "NORMAL", HealthState.RED, "0");
        coordinatorNode = createSearchUnit("coord1", "COORDINATOR", "NORMAL", HealthState.GREEN, "0");
    }
    
    
    @Test
    void testDeciderChain() {
        engine.enableDecider(HealthDecider.class);
        engine.enableDecider(RoleDecider.class);
        
        List<SearchUnit> candidates = Arrays.asList(healthyPrimary, unhealthyNode, coordinatorNode);
        List<SearchUnit> selected = engine.getAvailableNodesForAllocation("0", "test-index", candidates, NodeRole.PRIMARY);
        
        // Only healthy primary should pass
        assertThat(selected).hasSize(1);
        assertThat(selected).containsExactly(healthyPrimary);
    }
    
    @Test
    void testNoDecidersEnabled() {
        // With no deciders enabled, should return empty list
        List<SearchUnit> candidates = Arrays.asList(healthyPrimary);
        List<SearchUnit> selected = engine.getAvailableNodesForAllocation("0", "test-index", candidates, NodeRole.PRIMARY);
        
        assertThat(selected).isEmpty();
    }
    
    @Test
    void testEnableDisableDeciders() {
        engine.enableDecider(HealthDecider.class);
        
        List<SearchUnit> candidates = Arrays.asList(unhealthyNode);
        
        // With HealthDecider enabled, unhealthy node rejected
        List<SearchUnit> selected1 = engine.getAvailableNodesForAllocation("0", "test-index", candidates, NodeRole.PRIMARY);
        assertThat(selected1).isEmpty();
        
        // Disable HealthDecider
        engine.disableDecider(HealthDecider.class);
        
        // Still empty because no deciders enabled
        List<SearchUnit> selected2 = engine.getAvailableNodesForAllocation("0", "test-index", candidates, NodeRole.PRIMARY);
        assertThat(selected2).isEmpty();
        
        // Enable AllNodesDecider
        engine.enableDecider(AllNodesDecider.class);
        List<SearchUnit> selected3 = engine.getAvailableNodesForAllocation("0", "test-index", candidates, NodeRole.PRIMARY);
        assertThat(selected3).hasSize(1);
        assertThat(selected3).containsExactly(unhealthyNode);
    }
    
    @Test
    void testShardPoolFiltering() {
        SearchUnit wrongShardNode = createSearchUnit("wrong1", "PRIMARY", "NORMAL", HealthState.GREEN, "1");
        
        engine.enableDecider(ShardPoolDecider.class);
        
        List<SearchUnit> candidates = Arrays.asList(healthyPrimary, wrongShardNode);
        List<SearchUnit> selected = engine.getAvailableNodesForAllocation("0", "test-index", candidates, NodeRole.PRIMARY);
        
        // Should only include nodes from shard 0
        assertThat(selected).hasSize(1);
        assertThat(selected).containsExactly(healthyPrimary);
    }
    
    @Test
    void testComplexDeciderChain() {
        // Enable all deciders
        engine.enableDecider(HealthDecider.class);
        engine.enableDecider(RoleDecider.class);
        engine.enableDecider(ShardPoolDecider.class);
        
        SearchUnit perfectNode = createSearchUnit("perfect", "PRIMARY", "NORMAL", HealthState.GREEN, "0");
        SearchUnit wrongShardNode = createSearchUnit("wrong-shard", "PRIMARY", "NORMAL", HealthState.GREEN, "1");
        SearchUnit wrongRoleNode = createSearchUnit("wrong-role", "SEARCH_REPLICA", "NORMAL", HealthState.GREEN, "0");
        SearchUnit unhealthyNode = createSearchUnit("unhealthy", "PRIMARY", "NORMAL", HealthState.RED, "0");
        SearchUnit drainedNode = createSearchUnit("drained", "PRIMARY", "DRAIN", HealthState.GREEN, "0");
        
        List<SearchUnit> candidates = Arrays.asList(
            perfectNode, wrongShardNode, wrongRoleNode, unhealthyNode, drainedNode
        );
        
        List<SearchUnit> selected = engine.getAvailableNodesForAllocation("0", "test-index", candidates, NodeRole.PRIMARY);
        
        // Only perfect node should pass all deciders
        assertThat(selected).hasSize(1);
        assertThat(selected).containsExactly(perfectNode);
    }
    
    private SearchUnit createSearchUnit(String name, String role, String stateAdmin, HealthState statePulled, String shardId) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setRole(role);
        unit.setStateAdmin(stateAdmin);
        unit.setStatePulled(statePulled);
        unit.setShardId(shardId);
        return unit;
    }
}
