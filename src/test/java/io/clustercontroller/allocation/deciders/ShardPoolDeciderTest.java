package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ShardPoolDeciderTest {
    
    private ShardPoolDecider decider;
    
    @BeforeEach
    void setUp() {
        decider = new ShardPoolDecider();
    }
    
    @Test
    void testMatchingShardPool() {
        SearchUnit nodeInShard0 = createSearchUnit("node1", "0");
        SearchUnit nodeInShard1 = createSearchUnit("node2", "1");
        
        assertThat(decider.canAllocate("0", nodeInShard0, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.YES);
        assertThat(decider.canAllocate("1", nodeInShard1, "test-index", NodeRole.REPLICA))
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testMismatchedShardPool() {
        SearchUnit nodeInShard0 = createSearchUnit("node1", "0");
        SearchUnit nodeInShard1 = createSearchUnit("node2", "1");
        
        assertThat(decider.canAllocate("1", nodeInShard0, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
        assertThat(decider.canAllocate("0", nodeInShard1, "test-index", NodeRole.REPLICA))
            .isEqualTo(Decision.NO);
    }
    
    @Test
    void testNullShardId() {
        SearchUnit nodeWithNullShard = createSearchUnit("node1", null);
        
        assertThat(decider.canAllocate("0", nodeWithNullShard, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
    }
    
    private SearchUnit createSearchUnit(String name, String shardId) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setRole("primary");
        unit.setStateAdmin("NORMAL");
        unit.setStatePulled(HealthState.GREEN);
        unit.setShardId(shardId);
        return unit;
    }
}
