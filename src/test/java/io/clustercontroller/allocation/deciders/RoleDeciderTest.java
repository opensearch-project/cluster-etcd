package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RoleDeciderTest {
    
    private RoleDecider decider;
    
    @BeforeEach
    void setUp() {
        decider = new RoleDecider();
    }
    
    @Test
    void testPrimaryTargetRole() {
        SearchUnit primaryNode = createSearchUnit("node1", "PRIMARY");
        SearchUnit replicaNode = createSearchUnit("node2", "SEARCH_REPLICA");
        SearchUnit coordinatorNode = createSearchUnit("coord1", "COORDINATOR");
        
        // For PRIMARY target, only PRIMARY nodes allowed
        assertThat(decider.canAllocate("0", primaryNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.YES);
        assertThat(decider.canAllocate("0", replicaNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
        assertThat(decider.canAllocate("0", coordinatorNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
    }
    
    @Test
    void testReplicaTargetRole() {
        SearchUnit primaryNode = createSearchUnit("node1", "PRIMARY");
        SearchUnit replicaNode = createSearchUnit("node2", "SEARCH_REPLICA");
        SearchUnit coordinatorNode = createSearchUnit("coord1", "COORDINATOR");
        
        // For REPLICA target, only REPLICA nodes allowed
        assertThat(decider.canAllocate("0", primaryNode, "test-index", NodeRole.REPLICA))
            .isEqualTo(Decision.NO);
        assertThat(decider.canAllocate("0", replicaNode, "test-index", NodeRole.REPLICA))
            .isEqualTo(Decision.YES);
        assertThat(decider.canAllocate("0", coordinatorNode, "test-index", NodeRole.REPLICA))
            .isEqualTo(Decision.NO);
    }
    
    @Test
    void testCoordinatorAlwaysRejected() {
        SearchUnit coordinatorNode = createSearchUnit("coord1", "COORDINATOR");
        
        assertThat(decider.canAllocate("0", coordinatorNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
        assertThat(decider.canAllocate("0", coordinatorNode, "test-index", NodeRole.REPLICA))
            .isEqualTo(Decision.NO);
    }
    
    @Test
    void testNullRoleHandling() {
        SearchUnit nullRoleNode = createSearchUnit("node1", null);
        SearchUnit unknownRoleNode = createSearchUnit("node2", "unknown");
        
        assertThat(decider.canAllocate("0", nullRoleNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
        assertThat(decider.canAllocate("0", unknownRoleNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
    }
    
    private SearchUnit createSearchUnit(String name, String role) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setRole(role);
        unit.setStateAdmin("NORMAL");
        unit.setStatePulled(HealthState.GREEN);
        unit.setShardId("0");
        return unit;
    }
}
