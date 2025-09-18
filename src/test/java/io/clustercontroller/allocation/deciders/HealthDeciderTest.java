package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HealthDeciderTest {
    
    private HealthDecider decider;
    
    @BeforeEach
    void setUp() {
        decider = new HealthDecider();
    }
    
    @Test
    void testHealthyNodesAccepted() {
        SearchUnit greenNode = createSearchUnit("node1", "primary", "NORMAL", HealthState.GREEN);
        SearchUnit yellowNode = createSearchUnit("node2", "replica", "NORMAL", HealthState.YELLOW);
        
        assertThat(decider.canAllocate("0", greenNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.YES);
        assertThat(decider.canAllocate("0", yellowNode, "test-index", NodeRole.REPLICA))
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testUnhealthyNodesRejected() {
        SearchUnit redNode = createSearchUnit("node1", "primary", "NORMAL", HealthState.RED);
        SearchUnit drainedNode = createSearchUnit("node2", "primary", "DRAIN", HealthState.GREEN);
        SearchUnit nullAdminNode = createSearchUnit("node3", "primary", null, HealthState.GREEN);
        
        assertThat(decider.canAllocate("0", redNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
        assertThat(decider.canAllocate("0", drainedNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
        assertThat(decider.canAllocate("0", nullAdminNode, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
    }
    
    @Test
    void testDeciderProperties() {
        assertThat(decider.getName()).isEqualTo("HealthDecider");
        assertThat(decider.isEnabled()).isTrue();
    }
    
    private SearchUnit createSearchUnit(String name, String role, String stateAdmin, HealthState statePulled) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setRole(role);
        unit.setStateAdmin(stateAdmin);
        unit.setStatePulled(statePulled);
        unit.setShardId("0");
        return unit;
    }
}
