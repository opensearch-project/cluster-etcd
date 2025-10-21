package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AllNodesDeciderTest {
    
    private AllNodesDecider decider;
    
    @BeforeEach
    void setUp() {
        decider = new AllNodesDecider();
    }
    
    @Test
    void testAcceptsAllNodes() {
        SearchUnit primaryNode = createSearchUnit("node1", "PRIMARY", HealthState.GREEN);
        SearchUnit replicaNode = createSearchUnit("node2", "SEARCH_REPLICA", HealthState.YELLOW);
        SearchUnit coordinatorNode = createSearchUnit("coord1", "COORDINATOR", HealthState.GREEN);
        SearchUnit unhealthyNode = createSearchUnit("node3", "PRIMARY", HealthState.RED);
        
        assertThat(decider.canAllocate("0", primaryNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.YES);
        assertThat(decider.canAllocate("0", replicaNode, "test-index", NodeRole.REPLICA))
            .isEqualTo(Decision.YES);
        assertThat(decider.canAllocate("0", coordinatorNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.YES);
        assertThat(decider.canAllocate("0", unhealthyNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testDeciderProperties() {
        assertThat(decider.getName()).isEqualTo("AllNodesDecider");
        assertThat(decider.isEnabled()).isTrue();
        
        decider.setEnabled(false);
        assertThat(decider.isEnabled()).isFalse();
        
        decider.setEnabled(true);
        assertThat(decider.isEnabled()).isTrue();
    }
    
    private SearchUnit createSearchUnit(String name, String role, HealthState healthState) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setRole(role);
        unit.setStateAdmin("NORMAL");
        unit.setStatePulled(healthState);
        unit.setShardId("00");
        return unit;
    }
}
