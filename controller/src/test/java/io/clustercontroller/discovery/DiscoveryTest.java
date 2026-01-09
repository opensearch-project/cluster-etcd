package io.clustercontroller.discovery;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.metrics.MetricsProvider;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.store.MetadataStore;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive tests for Discovery flow.
 */
@ExtendWith(MockitoExtension.class)
class DiscoveryTest {
    
    @Mock
    private MetadataStore metadataStore;
    
    @Mock
    private MetricsProvider metricsProvider;
    
    @Mock
    private Counter mockCounter;
    
    private Discovery discovery;
    private static final String TEST_CLUSTER = "test-cluster";
    
    @BeforeEach
    void setUp() {
        discovery = new Discovery(metadataStore, metricsProvider);
        lenient().when(metricsProvider.counter(anyString(), anyMap())).thenReturn(mockCounter);
    }
    
    // =================================================================
    // SEARCH UNIT DISCOVERY TESTS
    // =================================================================
    
    @Test
    void testDiscoverSearchUnits_Success() throws Exception {
        // Given
        Map<String, SearchUnitActualState> actualStates = createMockActualStates();
        List<SearchUnit> existingUnits = new ArrayList<>();
        
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(actualStates);
        when(metadataStore.getAllSearchUnits(anyString())).thenReturn(existingUnits);
        when(metadataStore.getSearchUnit(anyString(), anyString())).thenReturn(Optional.empty());
        
        // When
        discovery.discoverSearchUnits(TEST_CLUSTER);
        
        // Then
        verify(metadataStore).getAllSearchUnitActualStates(anyString());
        verify(metadataStore, times(2)).getAllSearchUnits(anyString()); // Called in processAllSearchUnits and cleanupStaleSearchUnits
        verify(metadataStore, times(3)).getSearchUnit(anyString(), anyString()); // 3 units
        verify(metadataStore, times(3)).upsertSearchUnit(anyString(), anyString(), any(SearchUnit.class));
    }
    
    @Test
    void testDiscoverSearchUnits_UpdatesExistingUnits() throws Exception {
        // Given
        Map<String, SearchUnitActualState> actualStates = createMockActualStates();
        SearchUnit existingUnit = createMockSearchUnit("coordinator-node-1", "COORDINATOR");
        List<SearchUnit> existingUnits = List.of(existingUnit);
        
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(actualStates);
        when(metadataStore.getAllSearchUnits(anyString())).thenReturn(existingUnits);
        when(metadataStore.getSearchUnit(anyString(), eq("coordinator-node-1"))).thenReturn(Optional.of(existingUnit));
        when(metadataStore.getSearchUnit(anyString(), eq("primary-node-1"))).thenReturn(Optional.empty());
        when(metadataStore.getSearchUnit(anyString(), eq("replica-node-1"))).thenReturn(Optional.empty());
        
        // When
        discovery.discoverSearchUnits(TEST_CLUSTER);
        
        // Then
        // Verify discovery from etcd: 1 update + 2 creates
        verify(metadataStore, times(2)).upsertSearchUnit(anyString(), anyString(), any(SearchUnit.class)); // new units
        // Verify total updates: 1 from etcd discovery + 1 from processAllSearchUnits = 2 total
        verify(metadataStore, times(2)).updateSearchUnit(anyString(), any(SearchUnit.class));
    }
    
    @Test
    void testDiscoverSearchUnits_HandlesMetadataStoreError() throws Exception {
        // Given
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenThrow(new RuntimeException("Etcd connection failed"));
        when(metadataStore.getAllSearchUnits(anyString())).thenReturn(new ArrayList<>());
        
        // When & Then - should not throw exception
        assertThatCode(() -> discovery.discoverSearchUnits(TEST_CLUSTER)).doesNotThrowAnyException();
        
        verify(metadataStore).getAllSearchUnitActualStates(anyString());
        verify(metadataStore, times(2)).getAllSearchUnits(anyString()); // Called in processAllSearchUnits and cleanupStaleSearchUnits
    }
    
    // =================================================================
    // FETCH SEARCH UNITS FROM ETCD TESTS
    // =================================================================
    
    @Test
    void testFetchSearchUnitsFromEtcd_Success() throws Exception {
        // Given
        Map<String, SearchUnitActualState> actualStates = createMockActualStates();
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(actualStates);
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd(TEST_CLUSTER);
        
        // Then
        assertThat(result).hasSize(3);
        
        // Verify coordinator unit
        SearchUnit coordinator = result.stream()
                .filter(unit -> "COORDINATOR".equals(unit.getRole()))
                .findFirst()
                .orElseThrow();
        assertThat(coordinator.getName()).isEqualTo("coordinator-node-1");
        assertThat(coordinator.getHost()).isEqualTo("10.0.1.1");
        assertThat(coordinator.getPortHttp()).isEqualTo(9200);
        assertThat(coordinator.getRole()).isEqualTo("COORDINATOR");
        assertThat(coordinator.getShardId()).isEqualTo("COORDINATOR");
        assertThat(coordinator.getStatePulled()).isEqualTo(HealthState.GREEN);
        assertThat(coordinator.getStateAdmin()).isEqualTo("NORMAL");
        assertThat(coordinator.getNodeAttributes()).containsEntry("node.master", "true");
        assertThat(coordinator.getNodeAttributes()).containsEntry("node.data", "false");
        
        // Verify primary unit
        SearchUnit primary = result.stream()
                .filter(unit -> "PRIMARY".equals(unit.getRole()))
                .findFirst()
                .orElseThrow();
        assertThat(primary.getName()).isEqualTo("primary-node-1");
        assertThat(primary.getRole()).isEqualTo("PRIMARY");
        assertThat(primary.getShardId()).isEqualTo("shard-1");
        assertThat(primary.getNodeAttributes()).containsEntry("node.data", "true");
        assertThat(primary.getNodeAttributes()).containsEntry("node.ingest", "true");
        assertThat(primary.getNodeAttributes()).containsEntry("node.master", "false");
        
        // Verify replica unit
        SearchUnit replica = result.stream()
                .filter(unit -> "SEARCH_REPLICA".equals(unit.getRole()))
                .findFirst()
                .orElseThrow();
        assertThat(replica.getName()).isEqualTo("replica-node-1");
        assertThat(replica.getRole()).isEqualTo("SEARCH_REPLICA");
        assertThat(replica.getShardId()).isEqualTo("shard-2");
        assertThat(replica.getNodeAttributes()).containsEntry("node.data", "true");
        assertThat(replica.getNodeAttributes()).containsEntry("node.ingest", "false");
    }
    
    @Test
    void testFetchSearchUnitsFromEtcd_EmptyResult() throws Exception {
        // Given
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(new HashMap<>());
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd(TEST_CLUSTER);
        
        // Then
        assertThat(result).isEmpty();
        verify(metadataStore).getAllSearchUnitActualStates(anyString());
    }
    
    @Test
    void testFetchSearchUnitsFromEtcd_HandlesException() throws Exception {
        // Given
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenThrow(new RuntimeException("Connection error"));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd(TEST_CLUSTER);
        
        // Then
        assertThat(result).isEmpty();
    }
    
    @Test
    void testFetchSearchUnitsFromEtcd_HandlesNullRoleAndShardId() throws Exception {
        // Given
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState state = createHealthyActualState("test-node", "10.0.1.5", 9200, 9300);
        state.setRole(null); // null role
        state.setShardId(null); // null shard id
        actualStates.put("test-node", state);
        
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(actualStates);
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd(TEST_CLUSTER);
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.getFirst();
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
        SearchUnitActualState actualState = createHealthyActualState("coordinator-node-1", "10.0.1.1", 9200, 9300);
        actualState.setRole("COORDINATOR");
        actualState.setShardId("COORDINATOR");
        actualState.setClusterName("test-cluster");
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(Map.of("coordinator-node-1", actualState));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd(TEST_CLUSTER);
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.getFirst();
        assertThat(unit.getName()).isEqualTo("coordinator-node-1");
        assertThat(unit.getRole()).isEqualTo("COORDINATOR");
        assertThat(unit.getShardId()).isEqualTo("COORDINATOR");
        assertThat(unit.getHost()).isEqualTo("10.0.1.1");
        assertThat(unit.getPortHttp()).isEqualTo(9200);
        assertThat(unit.getNodeAttributes()).containsEntry("node.master", "true");
        assertThat(unit.getNodeAttributes()).containsEntry("node.data", "false");
        assertThat(unit.getNodeAttributes()).containsEntry("node.ingest", "false");
    }
    
    @Test
    void testConvertActualStateToSearchUnit_PrimaryNode() throws Exception {
        // Given
        SearchUnitActualState actualState = createHealthyActualState("primary-node-1", "10.0.1.2", 9200, 9300);
        actualState.setRole("PRIMARY");
        actualState.setShardId("shard-1");
        actualState.setClusterName("test-cluster");
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(Map.of("primary-node-1", actualState));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd(TEST_CLUSTER);
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.getFirst();
        assertThat(unit.getName()).isEqualTo("primary-node-1");
        assertThat(unit.getRole()).isEqualTo("PRIMARY");
        assertThat(unit.getShardId()).isEqualTo("shard-1");
        assertThat(unit.getNodeAttributes()).containsEntry("node.data", "true");
        assertThat(unit.getNodeAttributes()).containsEntry("node.ingest", "true");
        assertThat(unit.getNodeAttributes()).containsEntry("node.master", "false");
    }
    
    @Test
    void testConvertActualStateToSearchUnit_ReplicaNode() throws Exception {
        // Given
        SearchUnitActualState actualState = createHealthyActualState("replica-node-1", "10.0.1.3", 9200, 9300);
        actualState.setRole("SEARCH_REPLICA");
        actualState.setShardId("shard-2");
        actualState.setClusterName("test-cluster");
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(Map.of("replica-node-1", actualState));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd(TEST_CLUSTER);
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.getFirst();
        assertThat(unit.getName()).isEqualTo("replica-node-1");
        assertThat(unit.getRole()).isEqualTo("SEARCH_REPLICA");
        assertThat(unit.getShardId()).isEqualTo("shard-2");
        assertThat(unit.getNodeAttributes()).containsEntry("node.data", "true");
        assertThat(unit.getNodeAttributes()).containsEntry("node.ingest", "false");
        assertThat(unit.getNodeAttributes()).containsEntry("node.master", "false");
    }
    
    @Test
    void testConvertActualStateToSearchUnit_UnhealthyNode() throws Exception {
        // Given
        SearchUnitActualState actualState = createUnhealthyActualState("unhealthy-node-1", "10.0.1.4", 9200, 9300);
        actualState.setRole("PRIMARY");
        actualState.setShardId("shard-1");
        actualState.setClusterName("test-cluster");
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(Map.of("unhealthy-node-1", actualState));
        
        // When
        List<SearchUnit> result = discovery.fetchSearchUnitsFromEtcd(TEST_CLUSTER);
        
        // Then
        assertThat(result).hasSize(1);
        SearchUnit unit = result.getFirst();
        assertThat(unit.getStateAdmin()).isEqualTo("DRAIN");
        assertThat(unit.getStatePulled()).isEqualTo(HealthState.RED);
    }

    @Test
    void testCleanupStaleSearchUnits_MetricIncrementedWhenSearchUnitsDeleted() throws Exception {
        // Given: 2 stale search units (missing actual state)
        SearchUnit staleUnit1 = createMockSearchUnit("stale-node-1", "PRIMARY");
        SearchUnit staleUnit2 = createMockSearchUnit("stale-node-2", "SEARCH_REPLICA");
        SearchUnit healthyUnit = createMockSearchUnit("healthy-node-1", "PRIMARY");
        
        when(metadataStore.getAllSearchUnitActualStates(anyString())).thenReturn(new HashMap<>());
        when(metadataStore.getAllSearchUnits(TEST_CLUSTER))
            .thenReturn(List.of(staleUnit1, staleUnit2, healthyUnit));
        
        // staleUnit1 and staleUnit2 have no actual state (null)
        when(metadataStore.getSearchUnitActualState(TEST_CLUSTER, "stale-node-1")).thenReturn(null);
        when(metadataStore.getSearchUnitActualState(TEST_CLUSTER, "stale-node-2")).thenReturn(null);
        
        // healthyUnit has actual state with current timestamp
        SearchUnitActualState healthyState = createHealthyActualState("healthy-node-1", "10.0.1.5", 9200, 9300);
        healthyState.setTimestamp(System.currentTimeMillis()); // Current timestamp
        when(metadataStore.getSearchUnitActualState(TEST_CLUSTER, "healthy-node-1")).thenReturn(healthyState);

        discovery.discoverSearchUnits(TEST_CLUSTER);

        verify(metadataStore).deleteSearchUnit(TEST_CLUSTER, "stale-node-1");
        verify(metadataStore).deleteSearchUnit(TEST_CLUSTER, "stale-node-2");
        verify(metadataStore, never()).deleteSearchUnit(TEST_CLUSTER, "healthy-node-1");
        verify(metricsProvider, times(1)).counter(
            eq("discovery_cleaned_stale_search_units_count"),
            argThat(tags -> 
                tags.get("clusterId").equals(TEST_CLUSTER)
            )
        );
        verify(mockCounter).increment(2);
    }
    
    // =================================================================
    // HELPER METHODS
    // =================================================================
    
    private Map<String, SearchUnitActualState> createMockActualStates() {
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        
                // Coordinator node
        SearchUnitActualState coordinatorState = createHealthyActualState("coordinator-node-1", "10.0.1.1", 9200, 9300);
        coordinatorState.setRole("COORDINATOR");
        coordinatorState.setShardId("COORDINATOR");
        coordinatorState.setClusterName("test-cluster");
        actualStates.put("coordinator-node-1", coordinatorState);
        
        // Primary node  
        SearchUnitActualState primaryState = createHealthyActualState("primary-node-1", "10.0.1.2", 9200, 9300);
        primaryState.setRole("PRIMARY");
        primaryState.setShardId("shard-1");
        primaryState.setClusterName("test-cluster");
        actualStates.put("primary-node-1", primaryState);
        
        // Replica node
        SearchUnitActualState replicaState = createHealthyActualState("replica-node-1", "10.0.1.3", 9200, 9300);
        replicaState.setRole("SEARCH_REPLICA");
        replicaState.setShardId("shard-2");
        replicaState.setClusterName("test-cluster");
        actualStates.put("replica-node-1", replicaState);
        
        return actualStates;
    }
    
    private SearchUnitActualState createHealthyActualState(String nodeName, String address, int httpPort, int transportPort) {
        SearchUnitActualState state = new SearchUnitActualState();
        state.setNodeName(nodeName);
        state.setAddress(address);
        state.setHttpPort(httpPort);
        state.setTransportPort(transportPort);
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
    
    private SearchUnitActualState createUnhealthyActualState(String nodeName, String address, int httpPort, int transportPort) {
        SearchUnitActualState state = createHealthyActualState(nodeName, address, httpPort, transportPort);
        
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