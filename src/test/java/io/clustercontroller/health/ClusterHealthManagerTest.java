package io.clustercontroller.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.api.models.requests.ClusterInformationRequest;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.ShardState;
import io.clustercontroller.models.ClusterHealthInfo;
import io.clustercontroller.models.ClusterInformation;
import io.clustercontroller.models.ClusterControllerAssignment;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;



class ClusterHealthManagerTest {

    @Mock
    private MetadataStore metadataStore;

    private ClusterHealthManager clusterHealthManager;
    
    private final String testClusterId = "test-cluster";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        clusterHealthManager = new ClusterHealthManager(metadataStore);
    }

    @Test
    void testGetClusterHealth_WithHealthyNodes() throws Exception {
        // Given - healthy cluster with nodes
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState node1 = createHealthyDataNode("node1");
        actualStates.put("node1", node1);

        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(actualStates);
        when(metadataStore.getAllIndexConfigs(testClusterId))
            .thenReturn(Collections.emptyList());

        // When
        String healthJson = clusterHealthManager.getClusterHealth(testClusterId, "cluster");
        ClusterHealthInfo health = objectMapper.readValue(healthJson, ClusterHealthInfo.class);

        // Then
        assertThat(health.getClusterName()).isEqualTo(testClusterId);
        assertThat(health.getStatus()).isEqualTo(HealthState.GREEN);
        assertThat(health.getNumberOfNodes()).isEqualTo(1);
        assertThat(health.getNumberOfDataNodes()).isEqualTo(1);
        assertThat(health.getActiveNodes()).isEqualTo(1);
    }

    @Test
    void testGetClusterHealth_WithIndicesLevel() throws Exception {
        // Given - cluster with index
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState node1 = createNodeWithShards("node1", "test-index", 0, true, ShardState.STARTED);
        actualStates.put("node1", node1);

        Index index = createIndex("test-index", 1, Arrays.asList(1));
        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(actualStates);
        when(metadataStore.getAllIndexConfigs(testClusterId))
            .thenReturn(Arrays.asList(index));

        // When
        String healthJson = clusterHealthManager.getClusterHealth(testClusterId, "indices");
        ClusterHealthInfo health = objectMapper.readValue(healthJson, ClusterHealthInfo.class);

        // Then
        assertThat(health.getIndices()).isNotNull();
        assertThat(health.getIndices()).containsKey("test-index");
        assertThat(health.getIndices().get("test-index").getShards()).isNull(); // shards not included at 'indices' level
    }

    @Test
    void testGetClusterHealth_WithShardsLevel() throws Exception {
        // Given - cluster with shards
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState node1 = createNodeWithShards("node1", "test-index", 0, true, ShardState.STARTED);
        actualStates.put("node1", node1);

        Index index = createIndex("test-index", 1, Arrays.asList(1));
        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(actualStates);
        when(metadataStore.getAllIndexConfigs(testClusterId))
            .thenReturn(Arrays.asList(index));

        // When
        String healthJson = clusterHealthManager.getClusterHealth(testClusterId, "shards");
        ClusterHealthInfo health = objectMapper.readValue(healthJson, ClusterHealthInfo.class);

        // Then
        assertThat(health.getIndices()).isNotNull();
        assertThat(health.getIndices()).containsKey("test-index");
        assertThat(health.getIndices().get("test-index").getShards()).isNotNull();
        assertThat(health.getIndices().get("test-index").getShards()).isNotEmpty();
    }

    @Test
    void testGetIndexHealth_Success() throws Exception {
        // Given
        String indexName = "test-index";
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        SearchUnitActualState node1 = createNodeWithShards("node1", indexName, 0, true, ShardState.STARTED);
        actualStates.put("node1", node1);

        Index index = createIndex(indexName, 1, Arrays.asList(1));
        String indexJson = objectMapper.writeValueAsString(index);

        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(actualStates);
        when(metadataStore.getIndexConfig(testClusterId, indexName))
            .thenReturn(Optional.of(indexJson));

        // When
        String healthJson = clusterHealthManager.getIndexHealth(testClusterId, indexName, "indices");

        // Then
        assertThat(healthJson).isNotNull();
    }

    @Test
    void testGetIndexHealth_IndexNotFound() throws Exception {
        // Given
        String indexName = "non-existent-index";
        when(metadataStore.getIndexConfig(testClusterId, indexName))
            .thenReturn(Optional.empty());
        when(metadataStore.getAllSearchUnitActualStates(testClusterId))
            .thenReturn(Collections.emptyMap());

        // When/Then
        assertThatThrownBy(() -> clusterHealthManager.getIndexHealth(testClusterId, indexName, "indices"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Index '" + indexName + "' not found");
    }

    @Test
    void testGetClusterStats_NotImplemented() {
        // When/Then
        assertThatThrownBy(() -> clusterHealthManager.getClusterStats(testClusterId))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster stats not yet implemented");
    }

    // Helper methods to create test data
    private SearchUnitActualState createHealthyDataNode(String nodeName) {
        SearchUnitActualState state = new SearchUnitActualState();
        state.setNodeName(nodeName);
        state.setRole("REPLICA");
        // Set resource metrics that make isHealthy() return true
        // isHealthy() checks: memoryUsedPercent < 90 && diskAvailableMB > 1024
        state.setMemoryUsedMB(1000);
        state.setMemoryMaxMB(4000);
        state.setMemoryUsedPercent(25); // 25% < 90%
        state.setHeapUsedMB(500);
        state.setHeapMaxMB(2000);
        state.setHeapUsedPercent(25);
        state.setDiskTotalMB(10000);
        state.setDiskAvailableMB(5000); // 5000 > 1024
        state.setCpuUsedPercent(30);
        return state;
    }

    private SearchUnitActualState createNodeWithShards(String nodeName, String indexName, 
                                                       int shardId, boolean isPrimary, ShardState state) {
        SearchUnitActualState node = createHealthyDataNode(nodeName);
        
        SearchUnitActualState.ShardRoutingInfo shardInfo = new SearchUnitActualState.ShardRoutingInfo();
        shardInfo.setShardId(shardId);
        shardInfo.setRole(isPrimary ? "primary" : "replica");
        shardInfo.setState(state);
        
        Map<String, List<SearchUnitActualState.ShardRoutingInfo>> routing = new HashMap<>();
        routing.put(indexName, Arrays.asList(shardInfo));
        node.setNodeRouting(routing);
        
        return node;
    }

    private Index createIndex(String name, int numShards, List<Integer> replicaCounts) {
        Index index = new Index();
        index.setIndexName(name);
        
        IndexSettings settings = new IndexSettings();
        settings.setNumberOfShards(numShards);
        settings.setShardReplicaCount(replicaCounts);
        index.setSettings(settings);
        
        return index;
    }
    
    @Test
    void testGetClusterInformation_ClusterLocked_Success() throws Exception {
        // Given
        String controllerName = "controller-1";
        ClusterControllerAssignment assignment = new ClusterControllerAssignment();
        assignment.setController(controllerName);
        assignment.setCluster(testClusterId);
        assignment.setTimestamp(1761162295265L);
        assignment.setLease("694d9a0d5e11fca4");
        
        when(metadataStore.getAssignedController(testClusterId)).thenReturn(assignment);
        
        // When
        String infoJson = clusterHealthManager.getClusterInformation(testClusterId);
        ClusterInformation info = objectMapper.readValue(infoJson, ClusterInformation.class);
        
        // Then
        assertThat(info).isNotNull();
        assertThat(info.getClusterName()).isEqualTo(testClusterId);
        assertThat(info.getName()).isEqualTo(controllerName);
        assertThat(info.getTagline()).isEqualTo("The OpenSearch Project: https://opensearch.org/");

        // Verify metadata store was queried
        verify(metadataStore).getAssignedController(testClusterId);
    }

    @Test
    void testGetClusterInformation_ClusterNotLocked_ThrowsException() throws Exception {
        // Given - no controller assigned (getAssignedController returns null)
        when(metadataStore.getAssignedController(testClusterId)).thenReturn(null);
        
        // When/Then
        assertThatThrownBy(() -> clusterHealthManager.getClusterInformation(testClusterId))
            .isInstanceOf(Exception.class)
            .hasMessageContaining("Failed to get cluster information")
            .hasMessageContaining("Cluster is not associated with a controller");
        
        // Verify metadata store was queried
        verify(metadataStore).getAssignedController(testClusterId);
    }

    @Test
    void testGetClusterInformation_MetadataStoreThrowsException() throws Exception {
        // Given
        when(metadataStore.getAssignedController(testClusterId))
            .thenThrow(new RuntimeException("etcd connection failed"));
        
        // When/Then
        assertThatThrownBy(() -> clusterHealthManager.getClusterInformation(testClusterId))
            .isInstanceOf(Exception.class)
            .hasMessageContaining("Failed to get cluster information")
            .hasMessageContaining("etcd connection failed");
        
        // Verify metadata store was queried
        verify(metadataStore).getAssignedController(testClusterId);
    }

    @Test
    void testGetClusterInformation_WithVersion() throws Exception {
        // Given
        String controllerName = "controller-1";
        ClusterControllerAssignment assignment = new ClusterControllerAssignment();
        assignment.setController(controllerName);
        assignment.setCluster(testClusterId);
        
        // Mock version from registry
        ClusterInformation.Version version = new ClusterInformation.Version();
        version.setNumber("3.2.0");
        version.setDistribution("opensearch");
        version.setBuildType("tar");
        version.setHash("abc123");
        
        when(metadataStore.getAssignedController(testClusterId)).thenReturn(assignment);
        when(metadataStore.getClusterVersion(testClusterId)).thenReturn(version);
        
        // When
        String infoJson = clusterHealthManager.getClusterInformation(testClusterId);
        ClusterInformation info = objectMapper.readValue(infoJson, ClusterInformation.class);
        
        // Then
        assertThat(info).isNotNull();
        assertThat(info.getClusterName()).isEqualTo(testClusterId);
        assertThat(info.getName()).isEqualTo(controllerName);
        assertThat(info.getVersion()).isNotNull();
        assertThat(info.getVersion().getNumber()).isEqualTo("3.2.0");
        assertThat(info.getVersion().getDistribution()).isEqualTo("opensearch");
        assertThat(info.getVersion().getBuildType()).isEqualTo("tar");
        assertThat(info.getVersion().getHash()).isEqualTo("abc123");

        // Verify metadata store calls
        verify(metadataStore).getAssignedController(testClusterId);
        verify(metadataStore).getClusterVersion(testClusterId);
    }

    @Test
    void testGetClusterInformation_WithoutVersion() throws Exception {
        // Given
        String controllerName = "controller-1";
        ClusterControllerAssignment assignment = new ClusterControllerAssignment();
        assignment.setController(controllerName);
        assignment.setCluster(testClusterId);
        
        when(metadataStore.getAssignedController(testClusterId)).thenReturn(assignment);
        when(metadataStore.getClusterVersion(testClusterId)).thenReturn(null);
        
        // When
        String infoJson = clusterHealthManager.getClusterInformation(testClusterId);
        ClusterInformation info = objectMapper.readValue(infoJson, ClusterInformation.class);
        
        // Then
        assertThat(info).isNotNull();
        assertThat(info.getClusterName()).isEqualTo(testClusterId);
        assertThat(info.getName()).isEqualTo(controllerName);
        assertThat(info.getVersion()).isNull();

        // Verify metadata store calls
        verify(metadataStore).getAssignedController(testClusterId);
        verify(metadataStore).getClusterVersion(testClusterId);
    }

    @Test
    void testGetClusterInformation_VersionRetrievalFails() throws Exception {
        // Given
        String controllerName = "controller-1";
        ClusterControllerAssignment assignment = new ClusterControllerAssignment();
        assignment.setController(controllerName);
        assignment.setCluster(testClusterId);
        
        when(metadataStore.getAssignedController(testClusterId)).thenReturn(assignment);
        when(metadataStore.getClusterVersion(testClusterId))
            .thenThrow(new RuntimeException("Failed to read version"));
        
        // When - should not throw, version is optional
        String infoJson = clusterHealthManager.getClusterInformation(testClusterId);
        ClusterInformation info = objectMapper.readValue(infoJson, ClusterInformation.class);
        
        // Then - should succeed without version
        assertThat(info).isNotNull();
        assertThat(info.getClusterName()).isEqualTo(testClusterId);
        assertThat(info.getName()).isEqualTo(controllerName);
        assertThat(info.getVersion()).isNull();

        // Verify metadata store calls
        verify(metadataStore).getAssignedController(testClusterId);
        verify(metadataStore).getClusterVersion(testClusterId);
    }

    @Test
    void testSetClusterInformation_Success() throws Exception {
        // Given
        ClusterInformation.Version version = new ClusterInformation.Version();
        version.setNumber("3.2.0");
        version.setDistribution("opensearch");
        version.setBuildType("tar");
        
        ClusterInformationRequest request = ClusterInformationRequest.builder()
            .version(version)
            .build();
        
        // When
        clusterHealthManager.setClusterInformation(testClusterId, request);
        
        // Then
        verify(metadataStore).setClusterVersion(eq(testClusterId), any(ClusterInformation.Version.class));
    }

    @Test
    void testSetClusterInformation_ExtractsVersion() throws Exception {
        // Given
        ClusterInformation.Version version = new ClusterInformation.Version();
        version.setNumber("3.2.0");
        version.setDistribution("opensearch");
        version.setBuildType("tar");
        version.setHash("abc123");
        
        ClusterInformationRequest request = ClusterInformationRequest.builder()
            .version(version)
            .build();
        
        // When
        clusterHealthManager.setClusterInformation(testClusterId, request);
        
        // Then - verify version was passed to metadataStore
        verify(metadataStore).setClusterVersion(eq(testClusterId), argThat(v -> 
            v != null &&
            "3.2.0".equals(v.getNumber()) &&
            "opensearch".equals(v.getDistribution()) &&
            "tar".equals(v.getBuildType()) &&
            "abc123".equals(v.getHash())
        ));
    }

    @Test
    void testSetClusterInformation_NullVersion() throws Exception {
        // Given - request without version
        ClusterInformationRequest request = ClusterInformationRequest.builder()
            .build();
        
        // When/Then
        assertThatThrownBy(() -> clusterHealthManager.setClusterInformation(testClusterId, request))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Version information is required");
        
        // Verify no call to metadataStore
        verify(metadataStore, never()).setClusterVersion(any(), any());
    }

    @Test
    void testSetClusterInformation_MetadataStoreThrowsException() throws Exception {
        // Given
        ClusterInformation.Version version = new ClusterInformation.Version();
        version.setNumber("3.2.0");
        
        ClusterInformationRequest request = ClusterInformationRequest.builder()
            .version(version)
            .build();
        
        doThrow(new RuntimeException("etcd write failed"))
            .when(metadataStore).setClusterVersion(any(), any());
        
        // When/Then
        assertThatThrownBy(() -> clusterHealthManager.setClusterInformation(testClusterId, request))
            .isInstanceOf(Exception.class)
            .hasMessageContaining("Failed to set cluster information");
        
        // Verify call was attempted
        verify(metadataStore).setClusterVersion(eq(testClusterId), any(ClusterInformation.Version.class));
    }
}