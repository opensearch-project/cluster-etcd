package io.clustercontroller.enums;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for NodeRole enum.
 */
class NodeRoleTest {
    
    @Test
    void testFromStringValidValues() {
        assertThat(NodeRole.fromString("primary")).isEqualTo(NodeRole.PRIMARY);
        assertThat(NodeRole.fromString("replica")).isEqualTo(NodeRole.REPLICA);
        assertThat(NodeRole.fromString("coordinator")).isEqualTo(NodeRole.COORDINATOR);
        
        // Test case insensitive
        assertThat(NodeRole.fromString("PRIMARY")).isEqualTo(NodeRole.PRIMARY);
        assertThat(NodeRole.fromString("REPLICA")).isEqualTo(NodeRole.REPLICA);
        
        // Test with whitespace
        assertThat(NodeRole.fromString(" primary ")).isEqualTo(NodeRole.PRIMARY);
        assertThat(NodeRole.fromString("\treplica\n")).isEqualTo(NodeRole.REPLICA);
    }
    
    @Test
    void testFromStringInvalidValues() {
        assertThat(NodeRole.fromString("invalid")).isNull();
        assertThat(NodeRole.fromString("")).isNull();
        assertThat(NodeRole.fromString("   ")).isNull();
        assertThat(NodeRole.fromString(null)).isNull();
    }
    
    @Test
    void testGetValue() {
        assertThat(NodeRole.PRIMARY.getValue()).isEqualTo("primary");
        assertThat(NodeRole.REPLICA.getValue()).isEqualTo("replica");
        assertThat(NodeRole.COORDINATOR.getValue()).isEqualTo("coordinator");
    }
}
