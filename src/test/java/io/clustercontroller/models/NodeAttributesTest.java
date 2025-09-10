package io.clustercontroller.models;

import org.junit.jupiter.api.Test;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static io.clustercontroller.models.NodeAttributes.*;

/**
 * Tests for NodeAttributes utility class.
 */
class NodeAttributesTest {
    
    @Test
    void testNodeAttributeConstants() {
        assertThat(NODE_DATA).isEqualTo("node.data");
        assertThat(NODE_INGEST).isEqualTo("node.ingest");
        assertThat(NODE_MASTER).isEqualTo("node.master");
    }
    
    @Test
    void testRoleConstants() {
        assertThat(ROLE_COORDINATOR).isEqualTo("coordinator");
        assertThat(ROLE_PRIMARY).isEqualTo("primary");
        assertThat(ROLE_REPLICA).isEqualTo("replica");
    }
    
    @Test
    void testCoordinatorAttributes() {
        Map<String, String> attributes = COORDINATOR_ATTRIBUTES;
        assertThat(attributes).hasSize(3);
        assertThat(attributes.get(NODE_DATA)).isEqualTo("false");
        assertThat(attributes.get(NODE_INGEST)).isEqualTo("false");
        assertThat(attributes.get(NODE_MASTER)).isEqualTo("true");
    }
    
    @Test
    void testPrimaryAttributes() {
        Map<String, String> attributes = PRIMARY_ATTRIBUTES;
        assertThat(attributes).hasSize(3);
        assertThat(attributes.get(NODE_DATA)).isEqualTo("true");
        assertThat(attributes.get(NODE_INGEST)).isEqualTo("true");
        assertThat(attributes.get(NODE_MASTER)).isEqualTo("false");
    }
    
    @Test
    void testReplicaAttributes() {
        Map<String, String> attributes = REPLICA_ATTRIBUTES;
        assertThat(attributes).hasSize(3);
        assertThat(attributes.get(NODE_DATA)).isEqualTo("true");
        assertThat(attributes.get(NODE_INGEST)).isEqualTo("false");
        assertThat(attributes.get(NODE_MASTER)).isEqualTo("false");
    }
    
    @Test
    void testGetAttributesForRole() {
        // Test known roles
        assertThat(getAttributesForRole(ROLE_COORDINATOR)).isEqualTo(COORDINATOR_ATTRIBUTES);
        assertThat(getAttributesForRole(ROLE_PRIMARY)).isEqualTo(PRIMARY_ATTRIBUTES);
        assertThat(getAttributesForRole(ROLE_REPLICA)).isEqualTo(REPLICA_ATTRIBUTES);
        
        // Test unknown role
        assertThat(getAttributesForRole("unknown")).isEmpty();
        
        // Test null role
        assertThat(getAttributesForRole(null)).isEmpty();
    }
    
    @Test
    void testAttributeMapsAreImmutable() {
        // Verify that the predefined maps are immutable
        assertThatThrownBy(() -> COORDINATOR_ATTRIBUTES.put("new.key", "value"))
            .isInstanceOf(UnsupportedOperationException.class);
        
        assertThatThrownBy(() -> PRIMARY_ATTRIBUTES.put("new.key", "value"))
            .isInstanceOf(UnsupportedOperationException.class);
        
        assertThatThrownBy(() -> REPLICA_ATTRIBUTES.put("new.key", "value"))
            .isInstanceOf(UnsupportedOperationException.class);
    }
    
    @Test
    void testNodeAttributesClassCannotBeInstantiated() throws Exception {
        // Verify NodeAttributes is a proper utility class with private constructor
        var constructor = NodeAttributes.class.getDeclaredConstructor();
        assertThat(constructor.canAccess(null)).isFalse();
    }
}
