package io.clustercontroller.health;

import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class ClusterHealthManagerTest {

    @Mock
    private Discovery discovery;
    
    @Mock
    private MetadataStore metadataStore;
    
    private ClusterHealthManager healthManager;
    
    @BeforeEach
    void setUp() {
        healthManager = new ClusterHealthManager(discovery, metadataStore);
    }
    
    @Test
    void testGetClusterHealth_ThrowsUnsupportedOperation() {
        // When & Then
        assertThatThrownBy(() -> healthManager.getClusterHealth("cluster"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster health not yet implemented");
    }
    
    @Test
    void testGetClusterHealth_WithDifferentLevels() {
        // Test different health levels
        assertThatThrownBy(() -> healthManager.getClusterHealth("cluster"))
            .isInstanceOf(UnsupportedOperationException.class);
            
        assertThatThrownBy(() -> healthManager.getClusterHealth("indices"))
            .isInstanceOf(UnsupportedOperationException.class);
            
        assertThatThrownBy(() -> healthManager.getClusterHealth("shards"))
            .isInstanceOf(UnsupportedOperationException.class);
    }
    
    @Test
    void testGetIndexHealth_ThrowsUnsupportedOperation() {
        // Given
        String indexName = "test-index";
        String level = "indices";
        
        // When & Then
        assertThatThrownBy(() -> healthManager.getIndexHealth(indexName, level))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Index health not yet implemented");
    }
    
    @Test
    void testGetClusterStats_ThrowsUnsupportedOperation() {
        // When & Then
        assertThatThrownBy(() -> healthManager.getClusterStats())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster stats not yet implemented");
    }
    
    @Test
    void testConstructor_InitializesCorrectly() {
        // When
        ClusterHealthManager manager = new ClusterHealthManager(discovery, metadataStore);
        
        // Then
        assertThat(manager).isNotNull();
        // Verify dependencies are stored (via successful construction)
    }
}
