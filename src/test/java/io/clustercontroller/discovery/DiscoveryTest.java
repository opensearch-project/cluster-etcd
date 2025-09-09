package io.clustercontroller.discovery;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive tests for Discovery flow.
 */
@ExtendWith(MockitoExtension.class)
class DiscoveryTest {
    
    @Mock
    private MetadataStore metadataStore;
    
    private Discovery discovery;
    
    @BeforeEach
    void setUp() {
        discovery = new Discovery(metadataStore);
    }
    
    // =================================================================
    // SEARCH UNIT DISCOVERY TESTS
    // =================================================================
    
    @Test
    void testDiscoverSearchUnits_Success() throws Exception {
        // Given
        Map<String, SearchUnitActualState> actualStates = createMockActualStates();
        List<SearchUnit> existingUnits = new ArrayList<>();
        
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(actualStates);
        when(metadataStore.getAllSearchUnits()).thenReturn(existingUnits);
        when(metadataStore.getSearchUnit(anyString())).thenReturn(Optional.empty());
        
        // When
        discovery.discoverSearchUnits();
        
        // Then
        verify(metadataStore).getAllSearchUnitActualStates();
        verify(metadataStore).getAllSearchUnits();
        verify(metadataStore, times(3)).getSearchUnit(anyString()); // 3 units
        verify(metadataStore, times(3)).upsertSearchUnit(anyString(), any(SearchUnit.class));
    }
    
    @Test
    void testDiscoverSearchUnits_UpdatesExistingUnits() throws Exception {
        // Given
        Map<String, SearchUnitActualState> actualStates = createMockActualStates();
        SearchUnit existingUnit = createMockSearchUnit("coordinator-node-1", "coordinator");
        List<SearchUnit> existingUnits = Arrays.asList(existingUnit);
        
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(actualStates);
        when(metadataStore.getAllSearchUnits()).thenReturn(existingUnits);
        when(metadataStore.getSearchUnit("coordinator-node-1")).thenReturn(Optional.of(existingUnit));
        when(metadataStore.getSearchUnit("primary-node-1")).thenReturn(Optional.empty());
        when(metadataStore.getSearchUnit("replica-node-1")).thenReturn(Optional.empty());
        
        // When
        discovery.discoverSearchUnits();
        
        // Then
        // Verify discovery from etcd: 1 update + 2 creates
        verify(metadataStore, times(2)).upsertSearchUnit(anyString(), any(SearchUnit.class)); // new units
        // Verify total updates: 1 from etcd discovery + 1 from processAllSearchUnits = 2 total
        verify(metadataStore, times(2)).updateSearchUnit(any(SearchUnit.class));
    }
    
    @Test
    void testDiscoverSearchUnits_HandlesMetadataStoreError() throws Exception {
        // Given
        when(metadataStore.getAllSearchUnitActualStates()).thenThrow(new RuntimeException("Etcd connection failed"));
        when(metadataStore.getAllSearchUnits()).thenReturn(new ArrayList<>());
        
        // When & Then - should not throw exception
        assertThatCode(() -> discovery.discoverSearchUnits()).doesNotThrowAnyException();
        
        verify(metadataStore).getAllSearchUnitActualStates();
        verify(metadataStore).getAllSearchUnits();
    }
    
    // =================================================================
    // FETCH SEARCH UNITS FROM ETCD TESTS
    // =================================================================
    
    @Test
    void testFetchSearchUnitsFromEtcd_Success() throws Exception {
        // Given
        Map<String, SearchUnitActualState> actualStates = createMockActualStates();
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(actualStates);
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd();
        
        // Then
        assertThat(result).hasSize(3);
        
        // Verify coordinator unit
        SearchUnit coordinator = result.stream()
                .filter(unit -> "coordinator".equals(unit.getRole()))
                .findFirst()
                .orElseThrow();
        assertThat(coordinator.getName()).isEqualTo("coordinator-node-1");
        assertThat(coordinator.getHost()).isEqualTo("10.0.1.1");
        assertThat(coordinator.getPortHttp()).isEqualTo(9200);
        assertThat(coordinator.getRole()).isEqualTo("coordinator");
        assertThat(coordinator.getShardId()).isEqualTo("coordinator");
        assertThat(coordinator.getStatePulled()).isEqualTo(HealthState.GREEN);
        assertThat(coordinator.getStateAdmin()).isEqualTo("NORMAL");
        assertThat(coordinator.getNodeAttributes()).containsEntry("node.master", "true");
        assertThat(coordinator.getNodeAttributes()).containsEntry("node.data", "false");
        
        // Verify primary unit
        SearchUnit primary = result.stream()
                .filter(unit -> "primary".equals(unit.getRole()))
                .findFirst()
                .orElseThrow();
        assertThat(primary.getName()).isEqualTo("primary-node-1");
        assertThat(primary.getRole()).isEqualTo("primary");
        assertThat(primary.getShardId()).isEqualTo("shard-1");
        assertThat(primary.getNodeAttributes()).containsEntry("node.data", "true");
        assertThat(primary.getNodeAttributes()).containsEntry("node.ingest", "true");
        assertThat(primary.getNodeAttributes()).containsEntry("node.master", "false");
        
        // Verify replica unit
        SearchUnit replica = result.stream()
                .filter(unit -> "replica".equals(unit.getRole()))
                .findFirst()
                .orElseThrow();
        assertThat(replica.getName()).isEqualTo("replica-node-1");
        assertThat(replica.getRole()).isEqualTo("replica");
        assertThat(replica.getShardId()).isEqualTo("shard-2");
        assertThat(replica.getNodeAttributes()).containsEntry("node.data", "true");
        assertThat(replica.getNodeAttributes()).containsEntry("node.ingest", "false");
    }
    
    @Test
    void testFetchSearchUnitsFromEtcd_EmptyResult() throws Exception {
        // Given
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(new HashMap<>());
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd();
        
        // Then
        assertThat(result).isEmpty();
        verify(metadataStore).getAllSearchUnitActualStates();
    }
    
    @Test
    void testFetchSearchUnitsFromEtcd_HandlesException() throws Exception {
        // Given
        when(metadataStore.getAllSearchUnitActualStates()).thenThrow(new RuntimeException("Connection error"));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd();
        
        // Then
        assertThat(result).isEmpty();
    }
    
    @Test
    void testFetchSearchUnitsFromEtcd_HandlesNullRoleAndShardId() throws Exception {
        // Given
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState state = createHealthyActualState("test-node", "10.0.1.5", 9200);
        state.setRole(null); // null role
        state.setShardId(null); // null shard id
        actualStates.put("test-node", state);
        
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(actualStates);
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd();
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.get(0);
        assertThat(unit.getName()).isEqualTo("test-node");
        // Should handle null gracefully
        assertThat(unit.getRole()).isNull();
        assertThat(unit.getShardId()).isNull();
    }
    
    // =================================================================
    // STATE CONVERSION TESTS
    // =================================================================
    
    @Test
    void testConvertActualStateToSearchUnit_CoordinatorNode() throws Exception {
        // Given
        SearchUnitActualState actualState = createHealthyActualState("coordinator-node-1", "10.0.1.1", 9200);
        actualState.setRole("coordinator");
        actualState.setShardId("coordinator");
        actualState.setClusterName("test-cluster");
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(Map.of("coordinator-node-1", actualState));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd();
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.get(0);
        assertThat(unit.getName()).isEqualTo("coordinator-node-1");
        assertThat(unit.getRole()).isEqualTo("coordinator");
        assertThat(unit.getShardId()).isEqualTo("coordinator");
        assertThat(unit.getHost()).isEqualTo("10.0.1.1");
        assertThat(unit.getPortHttp()).isEqualTo(9200);
        assertThat(unit.getNodeAttributes()).containsEntry("node.master", "true");
        assertThat(unit.getNodeAttributes()).containsEntry("node.data", "false");
        assertThat(unit.getNodeAttributes()).containsEntry("node.ingest", "false");
    }
    
    @Test
    void testConvertActualStateToSearchUnit_PrimaryNode() throws Exception {
        // Given
        SearchUnitActualState actualState = createHealthyActualState("primary-node-1", "10.0.1.2", 9200);
        actualState.setRole("primary");
        actualState.setShardId("shard-1");
        actualState.setClusterName("test-cluster");
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(Map.of("primary-node-1", actualState));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd();
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.get(0);
        assertThat(unit.getName()).isEqualTo("primary-node-1");
        assertThat(unit.getRole()).isEqualTo("primary");
        assertThat(unit.getShardId()).isEqualTo("shard-1");
        assertThat(unit.getNodeAttributes()).containsEntry("node.data", "true");
        assertThat(unit.getNodeAttributes()).containsEntry("node.ingest", "true");
        assertThat(unit.getNodeAttributes()).containsEntry("node.master", "false");
    }
    
    @Test
    void testConvertActualStateToSearchUnit_ReplicaNode() throws Exception {
        // Given
        SearchUnitActualState actualState = createHealthyActualState("replica-node-1", "10.0.1.3", 9200);
        actualState.setRole("replica");
        actualState.setShardId("shard-2");
        actualState.setClusterName("test-cluster");
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(Map.of("replica-node-1", actualState));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd();
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.get(0);
        assertThat(unit.getName()).isEqualTo("replica-node-1");
        assertThat(unit.getRole()).isEqualTo("replica");
        assertThat(unit.getShardId()).isEqualTo("shard-2");
        assertThat(unit.getNodeAttributes()).containsEntry("node.data", "true");
        assertThat(unit.getNodeAttributes()).containsEntry("node.ingest", "false");
        assertThat(unit.getNodeAttributes()).containsEntry("node.master", "false");
    }
    
    @Test
    void testConvertActualStateToSearchUnit_UnhealthyNode() throws Exception {
        // Given
        SearchUnitActualState actualState = createUnhealthyActualState("unhealthy-node-1", "10.0.1.4", 9200);
        actualState.setRole("primary");
        actualState.setShardId("shard-1");
        actualState.setClusterName("test-cluster");
        when(metadataStore.getAllSearchUnitActualStates()).thenReturn(Map.of("unhealthy-node-1", actualState));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd();
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.get(0);
        assertThat(unit.getStateAdmin()).isEqualTo("DRAIN");
        assertThat(unit.getStatePulled()).isEqualTo(HealthState.RED);
    }
    
    // =================================================================
    // MONITORING TESTS
    // =================================================================
    
    @Test
    void testMonitorClusterHealth() {
        // When & Then - should not throw exception
        assertThatCode(() -> discovery.monitorClusterHealth()).doesNotThrowAnyException();
    }
    
    @Test
    void testUpdateClusterTopology() {
        // When & Then - should not throw exception
        assertThatCode(() -> discovery.updateClusterTopology()).doesNotThrowAnyException();
    }
    
    // =================================================================
    // HELPER METHODS
    // =================================================================
    
    private Map<String, SearchUnitActualState> createMockActualStates() {
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        
                // Coordinator node
        SearchUnitActualState coordinatorState = createHealthyActualState("coordinator-node-1", "10.0.1.1", 9200);
        coordinatorState.setRole("coordinator");
        coordinatorState.setShardId("coordinator");
        coordinatorState.setClusterName("test-cluster");
        actualStates.put("coordinator-node-1", coordinatorState);
        
        // Primary node  
        SearchUnitActualState primaryState = createHealthyActualState("primary-node-1", "10.0.1.2", 9200);
        primaryState.setRole("primary");
        primaryState.setShardId("shard-1");
        primaryState.setClusterName("test-cluster");
        actualStates.put("primary-node-1", primaryState);
        
        // Replica node
        SearchUnitActualState replicaState = createHealthyActualState("replica-node-1", "10.0.1.3", 9200);
        replicaState.setRole("replica");
        replicaState.setShardId("shard-2");
        replicaState.setClusterName("test-cluster");
        actualStates.put("replica-node-1", replicaState);
        
        return actualStates;
    }
    
    private SearchUnitActualState createHealthyActualState(String nodeName, String address, int port) {
        SearchUnitActualState state = new SearchUnitActualState();
        state.setNodeName(nodeName);
        state.setAddress(address);
        state.setPort(port);
        state.setNodeId("node-id-" + nodeName);
        state.setEphemeralId("ephemeral-" + nodeName);
        
        // Set healthy resource metrics
        state.setMemoryUsedMB(1000);
        state.setMemoryMaxMB(4000);
        state.setMemoryUsedPercent(25);
        state.setHeapUsedMB(500);
        state.setHeapMaxMB(2000);
        state.setHeapUsedPercent(25);
        state.setDiskTotalMB(100000);
        state.setDiskAvailableMB(80000);
        state.setCpuUsedPercent(20);
        
        // Set healthy resource metrics (memory < 90%, disk > 1024MB available)
        
        return state;
    }
    
    private SearchUnitActualState createUnhealthyActualState(String nodeName, String address, int port) {
        SearchUnitActualState state = createHealthyActualState(nodeName, address, port);
        
        // Set unhealthy resource metrics (memory >= 90% or disk <= 1024MB available)
        state.setMemoryUsedPercent(95);  // This will make isHealthy() return false
        state.setHeapUsedPercent(90);
        state.setCpuUsedPercent(95);
        state.setDiskAvailableMB(500);  // This will also make isHealthy() return false
        
        return state;
    }
    
    private SearchUnit createMockSearchUnit(String name, String role) {
        SearchUnit unit = new SearchUnit(name, role, "10.0.1.1");
        unit.setId(name);
        unit.setClusterName("test-cluster");
        unit.setShardId("shard-1");
        unit.setZone("zone-1");
        unit.setStatePulled(HealthState.GREEN);
        unit.setStateAdmin("NORMAL");
        return unit;
    }
}
