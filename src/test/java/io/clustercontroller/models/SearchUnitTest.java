package io.clustercontroller.models;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for SearchUnit model.
 */
class SearchUnitTest {
    
    @Test
    void testSearchUnitCreation() {
        SearchUnit unit = new SearchUnit();
        
        assertThat(unit.getNodeAttributes()).isNotNull();
        assertThat(unit.getNodeAttributes()).isEmpty();
        assertThat(unit.getPortHttp()).isEqualTo(9200);
        assertThat(unit.getPortTransport()).isEqualTo(9300);
    }
    
    @Test
    void testSearchUnitWithParameters() {
        String name = "search-unit-1";
        String role = "primary";
        String host = "host1.example.com";
        
        SearchUnit unit = new SearchUnit(name, role, host);
        
        assertThat(unit.getName()).isEqualTo(name);
        assertThat(unit.getRole()).isEqualTo(role);
        assertThat(unit.getHost()).isEqualTo(host);
        assertThat(unit.getStateAdmin()).isEqualTo("NORMAL");
    }
    
    @Test
    void testSearchUnitSetters() {
        SearchUnit unit = new SearchUnit();
        
        unit.setId("unit-123");
        unit.setRole("replica");
        unit.setClusterName("test-cluster");
        unit.setZone("zone-1");
        unit.setShardId("shard-0");
        unit.setStateAdmin("NORMAL");
        unit.setStatePulled(HealthState.YELLOW);
        
        assertThat(unit.getId()).isEqualTo("unit-123");
        assertThat(unit.getRole()).isEqualTo("replica");
        assertThat(unit.getClusterName()).isEqualTo("test-cluster");
        assertThat(unit.getZone()).isEqualTo("zone-1");
        assertThat(unit.getShardId()).isEqualTo("shard-0");
        assertThat(unit.getStateAdmin()).isEqualTo("NORMAL");
        assertThat(unit.getStatePulled()).isEqualTo(HealthState.YELLOW);
    }
    
    @Test
    void testSearchUnitToString() {
        SearchUnit unit = new SearchUnit("test-unit", "primary", "localhost");
        
        String unitString = unit.toString();
        
        assertThat(unitString).contains("test-unit");
        assertThat(unitString).contains("primary");
        assertThat(unitString).contains("localhost");
    }
}
