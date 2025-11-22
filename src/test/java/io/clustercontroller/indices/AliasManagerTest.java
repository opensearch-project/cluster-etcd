package io.clustercontroller.indices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.CoordinatorGoalState;
import io.clustercontroller.models.Alias;
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
        doNothing().when(metadataStore).setAlias(eq(testClusterId), eq(aliasName), any(Alias.class));
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act
        aliasManager.createAlias(testClusterId, aliasName, indexName, aliasConfig);
        
        // Assert - verify alias is stored
        verify(metadataStore).setAlias(eq(testClusterId), eq(aliasName),
            argThat(alias -> {
                return aliasName.equals(alias.getAliasName()) &&
                       indexName.equals(alias.getTargetIndices()) &&
                       alias.getCreatedAt() != null &&
                       alias.getUpdatedAt() != null;
            }));
        
        // Assert - verify coordinator goal state is updated
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
        doNothing().when(metadataStore).setAlias(eq(testClusterId), eq(aliasName), any(Alias.class));
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act
        aliasManager.createAlias(testClusterId, aliasName, indexName, aliasConfig);
        
        // Assert - verify alias is stored
        verify(metadataStore).setAlias(eq(testClusterId), eq(aliasName),
            argThat(alias -> {
                return aliasName.equals(alias.getAliasName()) &&
                       alias.getTargetIndices() instanceof List &&
                       ((List<?>) alias.getTargetIndices()).containsAll(Arrays.asList("test-monday", "test-tuesday")) &&
                       alias.getCreatedAt() != null &&
                       alias.getUpdatedAt() != null;
            }));
        
        // Assert - verify coordinator goal state is updated
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
    
    // ====================  POST API TESTS ====================
    
    @Test
    void testCreateAlias_MergesWithExistingSingleIndex() throws Exception {
        // Arrange - alias exists with single index
        String aliasName = "logs-current";
        String existingIndex = "test-monday";
        String newIndex = "test-tuesday";
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, existingIndex);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).setAlias(eq(testClusterId), eq(aliasName), any(Alias.class));
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act - add new index to existing alias
        aliasManager.createAlias(testClusterId, aliasName, newIndex, "{}");
        
        // Assert - should merge into list
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> {
                Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
                Object target = aliases.get(aliasName);
                return target instanceof List &&
                       ((List<?>) target).contains(existingIndex) &&
                       ((List<?>) target).contains(newIndex) &&
                       ((List<?>) target).size() == 2;
            }));
    }
    
    @Test
    void testCreateAlias_MergesWithExistingMultipleIndices() throws Exception {
        // Arrange - alias exists with multiple indices
        String aliasName = "logs-all";
        List<String> existingIndices = Arrays.asList("test-monday", "test-tuesday");
        String newIndex = "test-wednesday";
        
        when(metadataStore.getIndexConfig(eq(testClusterId), eq("test-wednesday"))).thenReturn(Optional.of("{}"));
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, new ArrayList<>(existingIndices));
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).setAlias(eq(testClusterId), eq(aliasName), any(Alias.class));
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act
        aliasManager.createAlias(testClusterId, aliasName, newIndex, "{}");
        
        // Assert - should add to existing list
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> {
                Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
                Object target = aliases.get(aliasName);
                return target instanceof List &&
                       ((List<?>) target).containsAll(existingIndices) &&
                       ((List<?>) target).contains(newIndex) &&
                       ((List<?>) target).size() == 3;
            }));
    }
    
    @Test
    void testCreateAlias_SkipsDuplicateIndex() throws Exception {
        // Arrange - alias already points to the index we're trying to add
        String aliasName = "logs-current";
        String indexName = "test-monday";
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, indexName);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).setAlias(eq(testClusterId), eq(aliasName), any(Alias.class));
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act - try to add same index again
        aliasManager.createAlias(testClusterId, aliasName, indexName, "{}");
        
        // Assert - should remain single index (not create list with duplicate)
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> {
                Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
                Object target = aliases.get(aliasName);
                return indexName.equals(target); // Still a String, not a List
            }));
    }
    
    @Test
    void testDeleteAlias_RemovesSpecificIndexFromMultiple() throws Exception {
        // Arrange - alias points to multiple indices
        String aliasName = "logs-all";
        List<String> indices = new ArrayList<>(Arrays.asList("test-monday", "test-tuesday", "test-wednesday"));
        
        when(metadataStore.getIndexConfig(eq(testClusterId), eq("test-wednesday"))).thenReturn(Optional.of("{}"));
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, indices);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).setAlias(eq(testClusterId), eq(aliasName), any(Alias.class));
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act - remove one index
        aliasManager.deleteAlias(testClusterId, aliasName, "test-tuesday");
        
        // Assert - should keep the other two indices
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> {
                Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
                Object target = aliases.get(aliasName);
                return target instanceof List &&
                       ((List<?>) target).contains("test-monday") &&
                       !((List<?>) target).contains("test-tuesday") &&
                       ((List<?>) target).contains("test-wednesday") &&
                       ((List<?>) target).size() == 2;
            }));
    }
    
    @Test
    void testDeleteAlias_ConvertsToSingleIndexWhenOnlyOneRemains() throws Exception {
        // Arrange - alias points to two indices
        String aliasName = "logs-recent";
        List<String> indices = new ArrayList<>(Arrays.asList("test-monday", "test-tuesday"));
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, indices);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).setAlias(eq(testClusterId), eq(aliasName), any(Alias.class));
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act - remove one index, leaving only one
        aliasManager.deleteAlias(testClusterId, aliasName, "test-tuesday");
        
        // Assert - should convert to single string
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> {
                Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
                Object target = aliases.get(aliasName);
                return "test-monday".equals(target); // Should be String, not List
            }));
    }
    
    @Test
    void testDeleteAlias_DeletesEntireAliasWhenLastIndexRemoved() throws Exception {
        // Arrange - alias points to single index
        String aliasName = "logs-current";
        String indexName = "test-monday";
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, indexName);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).deleteAlias(eq(testClusterId), eq(aliasName));
        doNothing().when(metadataStore).setCoordinatorGoalState(eq(testClusterId), any(CoordinatorGoalState.class));
        
        // Act - remove the only index
        aliasManager.deleteAlias(testClusterId, aliasName, indexName);
        
        // Assert - should delete entire alias
        verify(metadataStore).deleteAlias(eq(testClusterId), eq(aliasName));
        verify(metadataStore).setCoordinatorGoalState(eq(testClusterId),
            argThat(goalState -> !goalState.getRemoteShards().getAliases().containsKey(aliasName)));
    }
    
    @Test
    void testDeleteAlias_ThrowsExceptionForWrongIndex() throws Exception {
        // Arrange - alias points to different index
        String aliasName = "logs-current";
        String actualIndex = "test-monday";
        String requestedIndex = "test-tuesday";
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put(aliasName, actualIndex);
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        
        // Act & Assert - should throw exception
        Exception exception = assertThrows(Exception.class, 
            () -> aliasManager.deleteAlias(testClusterId, aliasName, requestedIndex));
        assertTrue(exception.getMessage().contains("does not point to index"));
    }
    
    @Test
    void testApplyAliasActions_AddAndRemoveActions() throws Exception {
        // Arrange
        when(metadataStore.getIndexConfig(eq(testClusterId), eq("test-wednesday"))).thenReturn(Optional.of("{}"));
        
        CoordinatorGoalState existingGoalState = new CoordinatorGoalState();
        existingGoalState.getRemoteShards().getAliases().put("old-alias", "test-monday");
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(existingGoalState);
        doNothing().when(metadataStore).setAlias(anyString(), anyString(), any(Alias.class));
        doNothing().when(metadataStore).deleteAlias(anyString(), anyString());
        doNothing().when(metadataStore).setCoordinatorGoalState(anyString(), any(CoordinatorGoalState.class));
        
        // Create actions
        List<Map<String, Map<String, String>>> actions = new ArrayList<>();
        
        // Action 1: Add new alias
        Map<String, Map<String, String>> addAction = new HashMap<>();
        Map<String, String> addDetails = new HashMap<>();
        addDetails.put("index", "test-tuesday");
        addDetails.put("alias", "new-alias");
        addAction.put("add", addDetails);
        actions.add(addAction);
        
        // Action 2: Remove old alias
        Map<String, Map<String, String>> removeAction = new HashMap<>();
        Map<String, String> removeDetails = new HashMap<>();
        removeDetails.put("index", "test-monday");
        removeDetails.put("alias", "old-alias");
        removeAction.put("remove", removeDetails);
        actions.add(removeAction);
        
        // Act
        Map<String, Object> result = aliasManager.applyAliasActions(testClusterId, actions);
        
        // Assert
        assertTrue((Boolean) result.get("acknowledged"));
        assertEquals(2, result.get("actionsCompleted"));
        assertEquals(0, result.get("actionsFailed"));
    }
    
    @Test
    void testApplyAliasActions_PartialFailure() throws Exception {
        // Arrange - one valid index, one invalid
        when(metadataStore.getIndexConfig(eq(testClusterId), eq("nonexistent"))).thenReturn(Optional.empty());
        
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(new CoordinatorGoalState());
        doNothing().when(metadataStore).setAlias(anyString(), anyString(), any(Alias.class));
        doNothing().when(metadataStore).setCoordinatorGoalState(anyString(), any(CoordinatorGoalState.class));
        
        List<Map<String, Map<String, String>>> actions = new ArrayList<>();
        
        // Action 1: Valid add
        Map<String, Map<String, String>> validAction = new HashMap<>();
        Map<String, String> validDetails = new HashMap<>();
        validDetails.put("index", "test-monday");
        validDetails.put("alias", "valid-alias");
        validAction.put("add", validDetails);
        actions.add(validAction);
        
        // Action 2: Invalid add (nonexistent index)
        Map<String, Map<String, String>> invalidAction = new HashMap<>();
        Map<String, String> invalidDetails = new HashMap<>();
        invalidDetails.put("index", "nonexistent");
        invalidDetails.put("alias", "invalid-alias");
        invalidAction.put("add", invalidDetails);
        actions.add(invalidAction);
        
        // Act
        Map<String, Object> result = aliasManager.applyAliasActions(testClusterId, actions);
        
        // Assert
        assertFalse((Boolean) result.get("acknowledged")); // Should be false due to failure
        assertEquals(1, result.get("actionsCompleted"));
        assertEquals(1, result.get("actionsFailed"));
        assertTrue(result.containsKey("errors"));
        @SuppressWarnings("unchecked")
        List<String> errors = (List<String>) result.get("errors");
        assertFalse(errors.isEmpty());
    }
    
    @Test
    void testApplyAliasActions_UnknownActionType() throws Exception {
        // Arrange
        when(metadataStore.getCoordinatorGoalState(testClusterId)).thenReturn(new CoordinatorGoalState());
        
        List<Map<String, Map<String, String>>> actions = new ArrayList<>();
        
        // Unknown action type
        Map<String, Map<String, String>> unknownAction = new HashMap<>();
        Map<String, String> details = new HashMap<>();
        details.put("index", "test-monday");
        details.put("alias", "some-alias");
        unknownAction.put("unknown", details); // Neither "add" nor "remove"
        actions.add(unknownAction);
        
        // Act
        Map<String, Object> result = aliasManager.applyAliasActions(testClusterId, actions);
        
        // Assert
        assertFalse((Boolean) result.get("acknowledged"));
        assertEquals(0, result.get("actionsCompleted"));
        assertEquals(1, result.get("actionsFailed"));
        assertTrue(result.containsKey("errors"));
    }
    
    @Test
    void testApplyAliasActions_EmptyActionsList() throws Exception {
        // Arrange
        List<Map<String, Map<String, String>>> actions = new ArrayList<>();
        
        // Act
        Map<String, Object> result = aliasManager.applyAliasActions(testClusterId, actions);
        
        // Assert
        assertTrue((Boolean) result.get("acknowledged"));
        assertEquals(0, result.get("actionsCompleted"));
        assertEquals(0, result.get("actionsFailed"));
    }
}
