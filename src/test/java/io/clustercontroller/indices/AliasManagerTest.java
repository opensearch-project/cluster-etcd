package io.clustercontroller.indices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.CoordinatorGoalState;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class AliasManagerTest {
    
    @Mock
    private MetadataStore metadataStore;
    
    private AliasManager aliasManager;
    private ObjectMapper objectMapper;
    private final String testClusterId = "test-cluster";
    
    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        objectMapper = new ObjectMapper();
        aliasManager = new AliasManager(metadataStore);
        
        // Mock index configs for test indices (getIndexConfig returns Optional<String>)
        // Use lenient() because not all tests need these mocks
        lenient().when(metadataStore.getIndexConfig(eq(testClusterId), eq("test-monday"))).thenReturn(Optional.of("{}"));
        lenient().when(metadataStore.getIndexConfig(eq(testClusterId), eq("test-tuesday"))).thenReturn(Optional.of("{}"));
    }
    
    @Test
    void testCreateAlias_SingleIndex() throws Exception {
        // Arrange
        String aliasName = "logs-current";
        String indexName = "test-monday";
        String aliasConfig = "{}";
        
        // Mock empty goal state initially
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(null);
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act
        aliasManager.createAlias(testClusterId, aliasName, indexName, aliasConfig);
        
        // Assert
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> {
                Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
                return aliases.containsKey(aliasName) && 
                       indexName.equals(aliases.get(aliasName));
            }));
    }
    
    @Test
    void testCreateAlias_MultipleIndices() throws Exception {
        // Arrange
        String aliasName = "logs-all";
        String indexName = "test-monday,test-tuesday";
        String aliasConfig = "{}";
        
        // Mock empty goal state initially
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(null);
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act
        aliasManager.createAlias(testClusterId, aliasName, indexName, aliasConfig);
        
        // Assert
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> {
                Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
                return aliases.containsKey(aliasName) && 
                       aliases.get(aliasName) instanceof List &&
                       ((List<?>) aliases.get(aliasName)).containsAll(Arrays.asList("test-monday", "test-tuesday"));
            }));
    }
    
    @Test
    void testCreateAlias_EmptyAliasName() throws Exception {
        // Arrange
        String aliasName = "";
        String indexName = "test-monday";
        String aliasConfig = "{}";
        
        // Act & Assert
        assertThrows(Exception.class, () -> aliasManager.createAlias(testClusterId, aliasName, indexName, aliasConfig));
    }
    
    @Test
    void testCreateAlias_EmptyTargetIndices() throws Exception {
        // Arrange
        String aliasName = "logs-current";
        String indexName = "";
        String aliasConfig = "{}";
        
        // Act & Assert
        assertThrows(Exception.class, () -> aliasManager.createAlias(testClusterId, aliasName, indexName, aliasConfig));
    }
    
    @Test
    void testCreateAlias_NonExistentIndex() throws Exception {
        // Arrange
        String aliasName = "logs-current";
        String indexName = "non-existent-index";
        String aliasConfig = "{}";
        
        when(metadataStore.getIndexConfig(eq(testClusterId), eq(indexName))).thenReturn(Optional.empty());
        
        // Act & Assert
        assertThrows(Exception.class, () -> aliasManager.createAlias(testClusterId, aliasName, indexName, aliasConfig));
    }
    
    @Test
    void testDeleteAlias_ExistingAlias() throws Exception {
        // Arrange
        String aliasName = "logs-current";
        String indexName = "test-monday";
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, indexName);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act
        aliasManager.deleteAlias(testClusterId, aliasName, indexName);
        
        // Assert
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> !goalState.getRemoteShards().getAliases().containsKey(aliasName)));
    }
    
    @Test
    void testDeleteAlias_NonExistentAlias() throws Exception {
        // Arrange
        String aliasName = "non-existent";
        String indexName = "test-monday";
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        
        // Act
        aliasManager.deleteAlias(testClusterId, aliasName, indexName);
        
        // Assert - should not throw exception, just log warning
        verify(metadataStore, never()).setCoordinatorGoalState(eq(testClusterId), any());
    }
    
    @Test
    void testCreateAlias_UpdateExisting() throws Exception {
        // Arrange - simulates updating an existing alias
        String aliasName = "logs-current";
        String oldIndexName = "test-monday";
        String newIndexName = "test-tuesday";
        String aliasConfig = "{}";
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, oldIndexName);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act - createAlias acts as upsert
        aliasManager.createAlias(testClusterId, aliasName, newIndexName, aliasConfig);
        
        // Assert
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> {
                Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
                return aliases.containsKey(aliasName) && 
                       newIndexName.equals(aliases.get(aliasName));
            }));
    }
    
    @Test
    void testGetAlias_ExistingAlias() throws Exception {
        // Arrange
        String aliasName = "logs-current";
        String indexName = "test-monday";
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, indexName);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        
        // Act
        String result = aliasManager.getAlias(testClusterId, aliasName);
        
        // Assert
        assertNotNull(result);
        assertTrue(result.contains(aliasName));
        assertTrue(result.contains(indexName));
    }
    
    @Test
    void testGetAlias_NonExistentAlias() throws Exception {
        // Arrange
        String aliasName = "non-existent";
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        
        // Act & Assert
        assertThrows(Exception.class, () -> aliasManager.getAlias(testClusterId, aliasName));
    }
    
    @Test
    void testGetAlias_NoGoalState() throws Exception {
        // Arrange
        String aliasName = "logs-current";
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(null);
        
        // Act & Assert
        assertThrows(Exception.class, () -> aliasManager.getAlias(testClusterId, aliasName));
    }
    
    @Test
    void testAliasExists_ExistingAlias() throws Exception {
        // Arrange
        String aliasName = "logs-current";
        String indexName = "test-monday";
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, indexName);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        
        // Act
        boolean exists = aliasManager.aliasExists(testClusterId, aliasName);
        
        // Assert
        assertTrue(exists);
    }
    
    @Test
    void testAliasExists_NonExistentAlias() throws Exception {
        // Arrange
        String aliasName = "non-existent";
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        
        // Act
        boolean exists = aliasManager.aliasExists(testClusterId, aliasName);
        
        // Assert
        assertFalse(exists);
    }
    
    @Test
    void testAliasExists_NoGoalState() throws Exception {
        // Arrange
        String aliasName = "logs-current";
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(null);
        
        // Act
        boolean exists = aliasManager.aliasExists(testClusterId, aliasName);
        
        // Assert
        assertFalse(exists);
    }
    
    @Test
    void testConstructor_InitializesCorrectly() {
        // When
        AliasManager manager = new AliasManager(metadataStore);
        
        // Then
        assertNotNull(manager);
    }
}
