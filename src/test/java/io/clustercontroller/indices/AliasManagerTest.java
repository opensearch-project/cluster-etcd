package io.clustercontroller.indices;

import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class AliasManagerTest {

    @Mock
    private MetadataStore metadataStore;
    
    private AliasManager aliasManager;
    private final String testClusterId = "test-cluster";
    
    @BeforeEach
    void setUp() {
        aliasManager = new AliasManager(metadataStore);
    }
    
    @Test
    void testCreateAlias_ThrowsUnsupportedOperation() {
        // Given
        String aliasName = "test-alias";
        String indexName = "test-index";
        String aliasConfig = "{\"filter\":{\"term\":{\"status\":\"published\"}}}";
        
        // When & Then
        assertThatThrownBy(() -> aliasManager.createAlias(testClusterId, aliasName, indexName, aliasConfig))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Alias creation not yet implemented");
    }
    
    @Test
    void testDeleteAlias_ThrowsUnsupportedOperation() {
        // Given
        String aliasName = "test-alias";
        String indexName = "test-index";
        
        // When & Then
        assertThatThrownBy(() -> aliasManager.deleteAlias(testClusterId, aliasName, indexName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Alias deletion not yet implemented");
    }
    
    @Test
    void testGetAlias_ThrowsUnsupportedOperation() {
        // Given
        String aliasName = "test-alias";
        
        // When & Then
        assertThatThrownBy(() -> aliasManager.getAlias(testClusterId, aliasName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Get alias not yet implemented");
    }
    
    @Test
    void testAliasExists_ThrowsUnsupportedOperation() {
        // Given
        String aliasName = "test-alias";
        
        // When & Then
        assertThatThrownBy(() -> aliasManager.aliasExists(testClusterId, aliasName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Alias existence check not yet implemented");
    }
    
    @Test
    void testConstructor_InitializesCorrectly() {
        // When
        AliasManager manager = new AliasManager(metadataStore);
        
        // Then
        assertThat(manager).isNotNull();
    }
}