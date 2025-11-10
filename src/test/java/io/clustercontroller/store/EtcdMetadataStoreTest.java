package io.clustercontroller.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.ClusterControllerAssignment;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.TaskMetadata;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.GetOption;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for EtcdMetadataStore.
 */
public class EtcdMetadataStoreTest {

    private static final String CLUSTER = "test-cluster";
    private static final String[] ENDPOINTS = new String[]{"http://localhost:2379"};

    private MockedStatic<Client> clientStaticMock;
    private Client mockEtcdClient;
    private KV mockKv;

    @BeforeEach
    public void setUp() throws Exception {
        // Static mock for Client.builder()
        clientStaticMock = Mockito.mockStatic(Client.class);

        // Mock the builder chain Client.builder().endpoints(...).build()
        ClientBuilder mockBuilder = mock(ClientBuilder.class);
        clientStaticMock.when(Client::builder).thenReturn(mockBuilder);
        when(mockBuilder.endpoints(any(String[].class))).thenReturn(mockBuilder);

        // Mock etcd Client and KV
        mockEtcdClient = mock(Client.class);
        mockKv = mock(KV.class);
        when(mockEtcdClient.getKVClient()).thenReturn(mockKv);
        when(mockBuilder.build()).thenReturn(mockEtcdClient);

        // Ensure singleton is reset before each test
        resetSingleton();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (clientStaticMock != null) clientStaticMock.close();
        resetSingleton();
    }

    // ------------------------- helpers -------------------------

    private void resetSingleton() throws Exception {
        Field f = EtcdMetadataStore.class.getDeclaredField("instance");
        f.setAccessible(true);
        f.set(null, null);
    }

    private EtcdMetadataStore newStore() throws Exception {
        return EtcdMetadataStore.createTestInstance(ENDPOINTS, "test-node", mockEtcdClient, mockKv);
    }

    private GetResponse mockGetResponse(List<KeyValue> kvs) {
        GetResponse resp = mock(GetResponse.class);
        when(resp.getKvs()).thenReturn(kvs);
        when(resp.getCount()).thenReturn((long) kvs.size());
        return resp;
    }

    private KeyValue mockKv(String valueUtf8) {
        KeyValue kv = mock(KeyValue.class);
        ByteSequence val = ByteSequence.from(valueUtf8, StandardCharsets.UTF_8);
        when(kv.getValue()).thenReturn(val);
        return kv;
    }

    private KeyValue mockKvWithKey(String keyUtf8, String valueUtf8) {
        KeyValue kv = mock(KeyValue.class);
        ByteSequence key = ByteSequence.from(keyUtf8, StandardCharsets.UTF_8);
        ByteSequence val = ByteSequence.from(valueUtf8, StandardCharsets.UTF_8);
        when(kv.getKey()).thenReturn(key);
        when(kv.getValue()).thenReturn(val);
        return kv;
    }

    private PutResponse mockPutResponse() {
        return mock(PutResponse.class);
    }

    private DeleteResponse mockDeleteResponse(long deleted) {
        DeleteResponse dr = mock(DeleteResponse.class, withSettings().lenient());
        doReturn(deleted).when(dr).getDeleted();
        return dr;
    }

    // ------------------------- singleton tests -------------------------

    @Test
    public void testSingletonGetInstance() throws Exception {
        EtcdMetadataStore s1 = newStore();
        // Don't call createTestInstance again as it resets the singleton
        // Instead, verify that the singleton is properly set
        assertThat(s1).isNotNull();
        assertThat(EtcdMetadataStore.getInstance()).isSameAs(s1);
    }

    @Test
    public void testGetInstanceNoArgsThrowsWhenUninitialized() {
        // Not calling newStore()
        assertThatThrownBy(() -> EtcdMetadataStore.getInstance())
            .isInstanceOf(IllegalStateException.class);
    }

    // ------------------------- tasks -------------------------

    @Test
    public void testGetAllTasksSortedByPriority() throws Exception {
        EtcdMetadataStore store = newStore();

        // Two tasks (priority 5 and 0), expect sorted ascending by priority
        String tHigh = "{\"name\":\"task-high\",\"priority\":5}";
        String tTop = "{\"name\":\"task-top\",\"priority\":0}";

        GetResponse resp = mockGetResponse(Arrays.asList(
                mockKv(tHigh),
                mockKv(tTop)
        ));

        when(mockKv.get(any(ByteSequence.class), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        List<TaskMetadata> tasks = store.getAllTasks(CLUSTER);

        assertThat(tasks).hasSize(2);
        assertThat(tasks.get(0).getName()).isEqualTo("task-top");
        assertThat(tasks.get(0).getPriority()).isEqualTo(0);
        assertThat(tasks.get(1).getName()).isEqualTo("task-high");
        assertThat(tasks.get(1).getPriority()).isEqualTo(5);
    }

    @Test
    public void testGetTaskFound() throws Exception {
        EtcdMetadataStore store = newStore();

        String tJson = "{\"name\":\"index-task\",\"priority\":3}";
        GetResponse resp = mockGetResponse(Collections.singletonList(mockKv(tJson)));
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<TaskMetadata> got = store.getTask(CLUSTER, "index-task");
        assertThat(got).isPresent();
        assertThat(got.get().getName()).isEqualTo("index-task");
        assertThat(got.get().getPriority()).isEqualTo(3);
    }

    @Test
    public void testGetTaskNotFound() throws Exception {
        EtcdMetadataStore store = newStore();

        GetResponse resp = mockGetResponse(Collections.emptyList());
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<TaskMetadata> got = store.getTask(CLUSTER, "missing");
        assertThat(got).isEmpty();
    }

    @Test
    public void testCreateTaskWritesJsonAndReturnsName() throws Exception {
        EtcdMetadataStore store = newStore();

        // Replace the store's ObjectMapper with a mock to control JSON
        ObjectMapper om = mock(ObjectMapper.class);
        setPrivateField(store, "objectMapper", om);

        TaskMetadata task = mock(TaskMetadata.class);
        when(task.getName()).thenReturn("cleanup-task");
        when(om.writeValueAsString(task)).thenReturn("{\"name\":\"cleanup-task\"}");

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        String name = store.createTask(CLUSTER, task);
        assertThat(name).isEqualTo("cleanup-task");
        verify(mockKv, times(1)).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testUpdateTaskPutsJson() throws Exception {
        EtcdMetadataStore store = newStore();

        ObjectMapper om = mock(ObjectMapper.class);
        setPrivateField(store, "objectMapper", om);

        TaskMetadata task = mock(TaskMetadata.class);
        when(task.getName()).thenReturn("maintenance-task");
        when(om.writeValueAsString(task)).thenReturn("{\"name\":\"maintenance-task\"}");

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        store.updateTask(CLUSTER, task);
        verify(mockKv, times(1)).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testDeleteTask() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock successful delete - we don't need to check the response since method returns void
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(DeleteResponse.class)));

        // Call delete method
        store.deleteTask(CLUSTER, "old-task");
        
        // Verify etcd delete was called with correct parameters
        verify(mockKv, times(1)).delete(any(ByteSequence.class));
    }

    @Test
    public void testDeleteTaskWithException() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock etcd failure
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd connection failed")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.deleteTask(CLUSTER, "failing-task"))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to delete task from etcd");
    }

    // ------------------------- search units -------------------------

    @Test
    public void testGetAllSearchUnits() throws Exception {
        EtcdMetadataStore store = newStore();

        String u1 = "{\"name\":\"node1\"}";
        String u2 = "{\"name\":\"node2\"}";

        GetResponse resp = mockGetResponse(Arrays.asList(
                mockKvWithKey("/test-cluster/search-unit/node1/conf", u1),
                mockKvWithKey("/test-cluster/search-unit/node2/conf", u2)
        ));
        when(mockKv.get(any(ByteSequence.class), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        List<SearchUnit> units = store.getAllSearchUnits(CLUSTER);
        assertThat(units).hasSize(2);
        assertThat(units.get(0).getName()).isIn("node1", "node2");
    }

    @Test
    public void testGetSearchUnitFound() throws Exception {
        EtcdMetadataStore store = newStore();

        String json = "{\"name\":\"node3\"}";
        GetResponse resp = mockGetResponse(Collections.singletonList(mockKv(json)));
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<SearchUnit> got = store.getSearchUnit(CLUSTER, "node3");
        assertThat(got).isPresent();
        assertThat(got.get().getName()).isEqualTo("node3");
    }

    @Test
    public void testGetSearchUnitNotFound() throws Exception {
        EtcdMetadataStore store = newStore();

        GetResponse resp = mockGetResponse(Collections.emptyList());
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<SearchUnit> got = store.getSearchUnit(CLUSTER, "missing");
        assertThat(got).isEmpty();
    }

    @Test
    public void testUpsertSearchUnit() throws Exception {
        EtcdMetadataStore store = newStore();

        ObjectMapper om = mock(ObjectMapper.class);
        setPrivateField(store, "objectMapper", om);

        SearchUnit unit = mock(SearchUnit.class);
        when(om.writeValueAsString(unit)).thenReturn("{\"name\":\"node4\"}");

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        store.upsertSearchUnit(CLUSTER, "node4", unit);
        verify(mockKv).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testUpdateSearchUnit() throws Exception {
        EtcdMetadataStore store = newStore();

        ObjectMapper om = mock(ObjectMapper.class);
        setPrivateField(store, "objectMapper", om);

        SearchUnit unit = mock(SearchUnit.class);
        when(unit.getName()).thenReturn("node5");
        when(om.writeValueAsString(unit)).thenReturn("{\"name\":\"node5\"}");

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        store.updateSearchUnit(CLUSTER, unit);
        verify(mockKv).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testDeleteSearchUnit() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock successful prefix delete for search unit (conf, goal-state, actual-state)
        when(mockKv.delete(any(ByteSequence.class), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(DeleteResponse.class)));

        // Call delete method
        store.deleteSearchUnit(CLUSTER, "node6");
        
        // Verify etcd prefix delete was called with correct parameters
        verify(mockKv).delete(any(ByteSequence.class), any());
    }

    @Test
    public void testDeleteSearchUnitWithException() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock etcd failure
        when(mockKv.delete(any(ByteSequence.class), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd timeout")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.deleteSearchUnit(CLUSTER, "node7"))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to delete search unit from etcd");
    }

    @Test
    public void testGetAllSearchUnitActualStates() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock actual-state entries with realistic etcd key paths
        // Path format: /<cluster>/search-unit/<unit-name>/actual-state
        String actualState1 = "{\"nodeName\":\"node1\",\"address\":\"10.0.1.1\",\"port\":9200}";
        String actualState2 = "{\"nodeName\":\"node2\",\"address\":\"10.0.1.2\",\"port\":9200}";
        String confEntry = "{\"name\":\"node3\"}"; // This should be ignored (not actual-state)

        GetResponse resp = mockGetResponse(Arrays.asList(
                mockKvWithKey("/test-cluster/search-unit/node1/actual-state", actualState1),
                mockKvWithKey("/test-cluster/search-unit/node2/actual-state", actualState2),
                mockKvWithKey("/test-cluster/search-unit/node3/conf", confEntry) // Should be skipped
        ));
        when(mockKv.get(any(ByteSequence.class), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Map<String, io.clustercontroller.models.SearchUnitActualState> actualStates = 
            store.getAllSearchUnitActualStates(CLUSTER);
        
        // Should only include the 2 actual-state entries, not the conf entry
        assertThat(actualStates).hasSize(2);
        assertThat(actualStates).containsKey("node1");
        assertThat(actualStates).containsKey("node2");
        assertThat(actualStates).doesNotContainKey("node3");
        
        // Verify the parsed data
        assertThat(actualStates.get("node1").getNodeName()).isEqualTo("node1");
        assertThat(actualStates.get("node1").getAddress()).isEqualTo("10.0.1.1");
        assertThat(actualStates.get("node2").getNodeName()).isEqualTo("node2");
    }

    // ------------------------- index configs -------------------------

    @Test
    public void testGetAllIndexConfigs() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock response with keys that end with /conf (index configs) and other keys (settings, mappings)
        GetResponse resp = mockGetResponse(Arrays.asList(
                mockKvWithKey("/test-cluster/indices/index1/conf", "{\"index_name\":\"index1\",\"settings\":{\"shard_replica_count\":[1,2],\"number_of_shards\":2}}"),
                mockKvWithKey("/test-cluster/indices/index1/settings", "{\"number_of_shards\":1,\"number_of_replicas\":2}"),
                mockKvWithKey("/test-cluster/indices/index2/conf", "{\"index_name\":\"index2\",\"settings\":{\"shard_replica_count\":[3],\"number_of_shards\":1}}"),
                mockKvWithKey("/test-cluster/indices/index2/mappings", "{\"properties\":{\"title\":{\"type\":\"text\"}}}")
        ));
        when(mockKv.get(any(ByteSequence.class), any(GetOption.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        List<Index> configs = store.getAllIndexConfigs(CLUSTER);
        // Should only return 2 configs (the /conf keys), not the settings/mappings
        assertThat(configs).hasSize(2);
        assertThat(configs.get(0).getIndexName()).isNotNull();
    }

    @Test
    public void testGetIndexConfigFound() throws Exception {
        EtcdMetadataStore store = newStore();

        String cfg = "{\"analysis\":{\"analyzer\":{\"std\":\"standard\"}}}";
        GetResponse resp = mockGetResponse(Collections.singletonList(mockKv(cfg)));
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<String> got = store.getIndexConfig(CLUSTER, "user-index");
        assertThat(got).isPresent();
        assertThat(got.get()).contains("analysis");
    }

    @Test
    public void testGetIndexConfigNotFound() throws Exception {
        EtcdMetadataStore store = newStore();

        GetResponse resp = mockGetResponse(Collections.emptyList());
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        Optional<String> got = store.getIndexConfig(CLUSTER, "missing");
        assertThat(got).isEmpty();
    }

    @Test
    public void testCreateIndexConfig() throws Exception {
        EtcdMetadataStore store = newStore();

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        String name = store.createIndexConfig(CLUSTER, "test-index", "{\"x\":1}");
        assertThat(name).isEqualTo("test-index");
        verify(mockKv).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testUpdateIndexConfig() throws Exception {
        EtcdMetadataStore store = newStore();

        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse()));

        store.updateIndexConfig(CLUSTER, "user-index", "{\"y\":2}");
        verify(mockKv).put(any(ByteSequence.class), any(ByteSequence.class));
    }

    @Test
    public void testDeleteIndexConfig() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock successful delete - we don't need to check the response since method returns void
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(DeleteResponse.class)));

        // Call delete method
        store.deleteIndexConfig(CLUSTER, "old-index");
        
        // Verify etcd delete was called with correct parameters
        verify(mockKv).delete(any(ByteSequence.class));
    }

    @Test
    public void testDeleteIndexConfigWithException() throws Exception {
        EtcdMetadataStore store = newStore();

        // Mock etcd failure
        when(mockKv.delete(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd network error")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.deleteIndexConfig(CLUSTER, "test-index"))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to delete index config from etcd");
    }

    @Test
    public void testSetIndexMappings() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String mappings = "{\"properties\": {\"field1\": {\"type\": \"text\"}}}";

        // Mock successful put response
        PutResponse mockPutResponse = mock(PutResponse.class);
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse));

        // Execute
        store.setIndexMappings("test-cluster", indexName, mappings);

        // Verify the put call was made with correct key and value
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        ArgumentCaptor<ByteSequence> valueCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).put(keyCaptor.capture(), valueCaptor.capture());

        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        String capturedValue = valueCaptor.getValue().toString(UTF_8);

        assertThat(capturedKey).contains("test-cluster/indices/test-index/mappings");
        assertThat(capturedValue).isEqualTo(mappings);
    }

    @Test
    public void testSetIndexMappingsWithException() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String mappings = "{\"properties\": {\"field1\": {\"type\": \"text\"}}}";

        // Mock etcd failure
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd connection failed")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.setIndexMappings("test-cluster", indexName, mappings))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to set index mappings in etcd");
    }

    @Test
    public void testSetIndexSettings() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String settings = "{\"number_of_shards\": 1, \"number_of_replicas\": 2}";

        // Mock successful put response
        PutResponse mockPutResponse = mock(PutResponse.class);
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse));

        // Execute
        store.setIndexSettings("test-cluster", indexName, settings);

        // Verify the put call was made with correct key and value (wrapped in "index" key)
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        ArgumentCaptor<ByteSequence> valueCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).put(keyCaptor.capture(), valueCaptor.capture());

        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        String capturedValue = valueCaptor.getValue().toString(UTF_8);

        assertThat(capturedKey).contains("test-cluster/indices/test-index/settings");
        // Settings should be wrapped in "index" key
        assertThat(capturedValue).contains("\"index\"");
        assertThat(capturedValue).contains("\"number_of_shards\"");
        assertThat(capturedValue).contains("\"number_of_replicas\"");
    }

    @Test
    public void testSetIndexSettingsWithException() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String settings = "{\"number_of_shards\": 1}";

        // Mock etcd failure
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd timeout")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.setIndexSettings("test-cluster", indexName, settings))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to set index settings in etcd");
    }

    @Test
    public void testGetIndexSettingsFound() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";
        String settingsJson = "{\"number_of_shards\": 3, \"shard_replica_count\": [2, 2, 2], \"pause_pull_ingestion\": false}";

        // Mock successful get response
        GetResponse resp = mockGetResponse(Collections.singletonList(mockKv(settingsJson)));
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        // Execute
        IndexSettings result = store.getIndexSettings("test-cluster", indexName);

        // Verify
        assertThat(result).isNotNull();
        assertThat(result.getNumberOfShards()).isEqualTo(3);
        assertThat(result.getShardReplicaCount()).containsExactly(2, 2, 2);
        assertThat(result.getPausePullIngestion()).isEqualTo(false);

        // Verify the get call was made with correct key
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).get(keyCaptor.capture());

        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        assertThat(capturedKey).contains("test-cluster/indices/test-index/settings");
    }

    @Test
    public void testGetIndexSettingsNotFound() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "non-existent-index";

        // Mock empty response (index settings not found)
        GetResponse resp = mockGetResponse(Collections.emptyList());
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        // Execute
        IndexSettings result = store.getIndexSettings("test-cluster", indexName);

        // Verify
        assertThat(result).isNull();

        // Verify the get call was made with correct key
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).get(keyCaptor.capture());

        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        assertThat(capturedKey).contains("test-cluster/indices/non-existent-index/settings");
    }

    @Test
    public void testGetIndexSettingsWithException() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "test-index";

        // Mock etcd failure
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd connection timeout")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.getIndexSettings("test-cluster", indexName))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to retrieve index settings from etcd");
    }

    @Test
    public void testGetIndexSettingsWithEmptySettings() throws Exception {
        EtcdMetadataStore store = newStore();
        String indexName = "empty-settings-index";
        String emptySettings = "{}";

        // Mock successful get response with empty settings
        GetResponse resp = mockGetResponse(Collections.singletonList(mockKv(emptySettings)));
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(resp));

        // Execute
        IndexSettings result = store.getIndexSettings("test-cluster", indexName);

        // Verify - empty JSON {} will parse to an IndexSettings object with all null fields
        assertThat(result).isNotNull();
        assertThat(result.getNumberOfShards()).isNull(); // No default value
        assertThat(result.getShardReplicaCount()).isNull();
        assertThat(result.getPausePullIngestion()).isNull();
    }

    // ------------------------- lifecycle -------------------------

    @Test
    public void testCloseClosesEtcdClient() throws Exception {
        EtcdMetadataStore store = newStore();
        store.close();
        verify(mockEtcdClient, times(1)).close();
    }

    // ------------------------- shard allocation tests -------------------------

    @Test
    public void testGetPlannedAllocationSuccess() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";

        // Mock ShardAllocation response
        ShardAllocation expectedAllocation = new ShardAllocation(shardId, indexName);
        expectedAllocation.setIngestSUs(Arrays.asList("node1"));
        expectedAllocation.setSearchSUs(Arrays.asList("node2", "node3"));

        String allocationJson = new ObjectMapper().writeValueAsString(expectedAllocation);
        ByteSequence mockValue = ByteSequence.from(allocationJson, UTF_8);

        KeyValue mockKeyValue = mock(KeyValue.class);
        when(mockKeyValue.getValue()).thenReturn(mockValue);

        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getKvs()).thenReturn(Arrays.asList(mockKeyValue));

        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        // Execute
        ShardAllocation result = store.getPlannedAllocation(clusterId, indexName, shardId);

        // Verify
        assertThat(result).isNotNull();
        assertThat(result.getShardId()).isEqualTo(shardId);
        assertThat(result.getIndexName()).isEqualTo(indexName);
        assertThat(result.getIngestSUs()).containsExactly("node1");
        assertThat(result.getSearchSUs()).containsExactly("node2", "node3");
    }

    @Test
    public void testGetPlannedAllocationNotFound() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";

        // Mock empty response
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getKvs()).thenReturn(Collections.emptyList());

        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        // Execute
        ShardAllocation result = store.getPlannedAllocation(clusterId, indexName, shardId);

        // Verify
        assertThat(result).isNull();
    }

    @Test
    public void testGetPlannedAllocationWithException() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";

        // Mock etcd failure
        when(mockKv.get(any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd timeout")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.getPlannedAllocation(clusterId, indexName, shardId))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("etcd timeout");
    }

    @Test
    public void testSetPlannedAllocationSuccess() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";

        ShardAllocation allocation = new io.clustercontroller.models.ShardAllocation(shardId, indexName);
        allocation.setIngestSUs(Arrays.asList("node1"));
        allocation.setSearchSUs(Arrays.asList("node2", "node3"));

        PutResponse mockPutResponse = mock(PutResponse.class);
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(mockPutResponse));

        // Execute
        store.setPlannedAllocation(clusterId, indexName, shardId, allocation);

        // Verify the put call was made with correct key and value
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        ArgumentCaptor<ByteSequence> valueCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).put(keyCaptor.capture(), valueCaptor.capture());

        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        String capturedValue = valueCaptor.getValue().toString(UTF_8);

        assertThat(capturedKey).contains("test-cluster/indices/test-index/0/planned-allocation");
        assertThat(capturedValue).contains("\"shard_id\":\"0\"");
        assertThat(capturedValue).contains("\"index_name\":\"test-index\"");
        assertThat(capturedValue).contains("\"ingest_sus\":[\"node1\"]");
        assertThat(capturedValue).contains("\"search_sus\":[\"node2\",\"node3\"]");
    }

    @Test
    public void testSetPlannedAllocationWithException() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";

        ShardAllocation allocation = new io.clustercontroller.models.ShardAllocation(shardId, indexName);

        // Mock etcd failure
        when(mockKv.put(any(ByteSequence.class), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd timeout")));

        // Verify exception is propagated
        assertThatThrownBy(() -> store.setPlannedAllocation(clusterId, indexName, shardId, allocation))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("etcd timeout");
    }

    @Test
    void testSetSearchUnitGoalStateConcurrentModification() throws Exception {
        // Setup
        String clusterId = "test-cluster";
        String unitName = "test-node";
        SearchUnitGoalState goalState1 = new SearchUnitGoalState();
        SearchUnitGoalState goalState2 = new SearchUnitGoalState();
        
        // Add different shard allocations to make them distinct
        goalState1.getLocalShards().put("index1", Map.of("0", "PRIMARY"));
        goalState2.getLocalShards().put("index2", Map.of("0", "SEARCH_REPLICA"));

        // Create store instance
        EtcdMetadataStore store = newStore();

        // Mock etcd client for CAS behavior
        KV mockKvClient = mock(KV.class);
        setPrivateField(store, "kvClient", mockKvClient);

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getSearchUnitGoalStatePath(clusterId, unitName))
            .thenReturn("/test-cluster/search-units/test-node/goal-state");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Simulate concurrent modification scenario
        ByteSequence keyBytes = ByteSequence.from("/test-cluster/search-units/test-node/goal-state", UTF_8);
        
        // First call: GET returns current revision
        GetResponse getResponse = mock(GetResponse.class);
        KeyValue mockKv = mock(KeyValue.class);
        when(mockKv.getModRevision()).thenReturn(100L);
        when(getResponse.getCount()).thenReturn(1L);
        when(getResponse.getKvs()).thenReturn(List.of(mockKv));
        
        CompletableFuture<GetResponse> getFuture = CompletableFuture.completedFuture(getResponse);
        when(mockKvClient.get(keyBytes)).thenReturn(getFuture);

        // Second call: Transaction fails (concurrent modification)
        Txn mockTxn = mock(Txn.class);
        TxnResponse mockTxnResponse = mock(TxnResponse.class);
        when(mockTxnResponse.isSucceeded()).thenReturn(false); // CAS failed
        
        CompletableFuture<TxnResponse> txnFuture = CompletableFuture.completedFuture(mockTxnResponse);
        when(mockTxn.commit()).thenReturn(txnFuture);
        when(mockTxn.Else(any())).thenReturn(mockTxn);
        when(mockTxn.Then(any())).thenReturn(mockTxn);
        when(mockTxn.If(any())).thenReturn(mockTxn);
        when(mockKvClient.txn()).thenReturn(mockTxn);

        // Test: Should throw RuntimeException due to concurrent modification
        assertThatThrownBy(() -> store.setSearchUnitGoalState(clusterId, unitName, goalState1))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to update goal state for test-node due to concurrent modification");

        // Verify the CAS operation was attempted
        verify(mockKvClient).get(keyBytes);
        verify(mockKvClient).txn();
        verify(mockTxn).If(any());
        verify(mockTxn).Then(any());
        verify(mockTxn).Else(any());
        verify(mockTxn).commit();
    }

    @Test
    void testSetSearchUnitGoalStateSuccessfulCAS() throws Exception {
        // Setup
        String clusterId = "test-cluster";
        String unitName = "test-node";
        SearchUnitGoalState goalState = new SearchUnitGoalState();
        goalState.getLocalShards().put("index1", Map.of("0", "PRIMARY"));

        // Create store instance
        EtcdMetadataStore store = newStore();

        // Mock etcd client for successful CAS
        KV mockKvClient = mock(KV.class);
        setPrivateField(store, "kvClient", mockKvClient);

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getSearchUnitGoalStatePath(clusterId, unitName))
            .thenReturn("/test-cluster/search-units/test-node/goal-state");
        setPrivateField(store, "pathResolver", mockPathResolver);

        ByteSequence keyBytes = ByteSequence.from("/test-cluster/search-units/test-node/goal-state", UTF_8);
        
        // First call: GET returns current revision
        GetResponse getResponse = mock(GetResponse.class);
        KeyValue mockKv = mock(KeyValue.class);
        when(mockKv.getModRevision()).thenReturn(100L);
        when(getResponse.getCount()).thenReturn(1L);
        when(getResponse.getKvs()).thenReturn(List.of(mockKv));
        
        CompletableFuture<GetResponse> getFuture = CompletableFuture.completedFuture(getResponse);
        when(mockKvClient.get(keyBytes)).thenReturn(getFuture);

        // Second call: Transaction succeeds
        Txn mockTxn = mock(Txn.class);
        TxnResponse mockTxnResponse = mock(TxnResponse.class);
        when(mockTxnResponse.isSucceeded()).thenReturn(true); // CAS succeeded
        
        CompletableFuture<TxnResponse> txnFuture = CompletableFuture.completedFuture(mockTxnResponse);
        when(mockTxn.commit()).thenReturn(txnFuture);
        when(mockTxn.Else(any())).thenReturn(mockTxn);
        when(mockTxn.Then(any())).thenReturn(mockTxn);
        when(mockTxn.If(any())).thenReturn(mockTxn);
        when(mockKvClient.txn()).thenReturn(mockTxn);

        // Test: Should succeed without throwing exception
        assertThatCode(() -> store.setSearchUnitGoalState(clusterId, unitName, goalState))
                .doesNotThrowAnyException();

        // Verify the CAS operation was performed
        verify(mockKvClient).get(keyBytes);
        verify(mockKvClient).txn();
        verify(mockTxn).If(any());
        verify(mockTxn).Then(any());
        verify(mockTxn).Else(any());
        verify(mockTxn).commit();
    }

    // ------------------------- getAssignedController tests -------------------------

    @Test
    public void testGetAssignedController_ControllerAssigned() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";
        String controllerName = "controller-1";
        String leaseId = "694d9a0d5e11fca4";
        long timestamp = 1761162295265L;

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getClusterAssignedControllerPath(clusterId))
            .thenReturn("/test-cluster/assigned-controller");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Create ClusterControllerAssignment JSON
        ClusterControllerAssignment assignment = new ClusterControllerAssignment();
        assignment.setController(controllerName);
        assignment.setCluster(clusterId);
        assignment.setTimestamp(timestamp);
        assignment.setLease(leaseId);
        
        String metadataJson = new ObjectMapper().writeValueAsString(assignment);

        // Mock GetResponse with controller metadata
        KeyValue mockKeyValue = mock(KeyValue.class);
        when(mockKeyValue.getValue()).thenReturn(ByteSequence.from(metadataJson, UTF_8));
        
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getKvs()).thenReturn(List.of(mockKeyValue));

        when(mockKv.get(any(ByteSequence.class)))
            .thenReturn(CompletableFuture.completedFuture(mockResponse));

        // Execute
        ClusterControllerAssignment result = store.getAssignedController(clusterId);

        // Verify
        assertThat(result).isNotNull();
        assertThat(result.getController()).isEqualTo(controllerName);
        assertThat(result.getCluster()).isEqualTo(clusterId);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(result.getLease()).isEqualTo(leaseId);
        
        // Verify the get call was made with correct key
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).get(keyCaptor.capture());
        
        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        assertThat(capturedKey).isEqualTo("/test-cluster/assigned-controller");
    }

    @Test
    public void testGetAssignedController_NoControllerAssigned() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getClusterAssignedControllerPath(clusterId))
            .thenReturn("/test-cluster/assigned-controller");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Mock GetResponse with empty key-value pairs (no controller assigned)
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getKvs()).thenReturn(Collections.emptyList());

        when(mockKv.get(any(ByteSequence.class)))
            .thenReturn(CompletableFuture.completedFuture(mockResponse));

        // Execute
        ClusterControllerAssignment result = store.getAssignedController(clusterId);

        // Verify - should return null when no controller is assigned
        assertThat(result).isNull();
        
        // Verify the get call was made with correct key
        ArgumentCaptor<ByteSequence> keyCaptor = ArgumentCaptor.forClass(ByteSequence.class);
        verify(mockKv).get(keyCaptor.capture());
        
        String capturedKey = keyCaptor.getValue().toString(UTF_8);
        assertThat(capturedKey).isEqualTo("/test-cluster/assigned-controller");
    }

    @Test
    public void testGetAssignedController_EtcdException() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getClusterAssignedControllerPath(clusterId))
            .thenReturn("/test-cluster/assigned-controller");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Mock etcd failure
        when(mockKv.get(any(ByteSequence.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd connection timeout")));

        // Verify exception is thrown
        assertThatThrownBy(() -> store.getAssignedController(clusterId))
            .isInstanceOf(Exception.class)
            .hasMessageContaining("Failed to get assigned controller")
            .hasMessageContaining("etcd connection timeout");
        
        // Verify the get call was attempted
        verify(mockKv).get(any(ByteSequence.class));
    }

    @Test
    public void testGetAssignedController_InvalidJson() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getClusterAssignedControllerPath(clusterId))
            .thenReturn("/test-cluster/assigned-controller");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Mock GetResponse with invalid JSON
        String invalidJson = "{invalid json}";
        KeyValue mockKeyValue = mock(KeyValue.class);
        when(mockKeyValue.getValue()).thenReturn(ByteSequence.from(invalidJson, UTF_8));
        
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getKvs()).thenReturn(List.of(mockKeyValue));

        when(mockKv.get(any(ByteSequence.class)))
            .thenReturn(CompletableFuture.completedFuture(mockResponse));

        // Verify exception is thrown due to JSON parsing error
        assertThatThrownBy(() -> store.getAssignedController(clusterId))
            .isInstanceOf(Exception.class)
            .hasMessageContaining("Failed to get assigned controller");
        
        // Verify the get call was made
        verify(mockKv).get(any(ByteSequence.class));
    }

    // ------------------------- getClusterVersion tests -------------------------

    @Test
    public void testGetClusterVersion_Success() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getClusterRegistryPath(clusterId))
            .thenReturn("/multi-cluster/clusters/test-cluster/metadata");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Mock GetResponse with cluster metadata containing version
        String metadataJson = "{"
            + "\"cluster_id\":\"test-cluster\","
            + "\"created_at\":\"2024-01-01\","
            + "\"version\":{"
            + "\"number\":\"3.2.0\","
            + "\"distribution\":\"opensearch\","
            + "\"build_type\":\"tar\","
            + "\"build_hash\":\"abc123\""
            + "}"
            + "}";
        
        KeyValue mockKeyValue = mock(KeyValue.class);
        when(mockKeyValue.getValue()).thenReturn(ByteSequence.from(metadataJson, UTF_8));
        
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getKvs()).thenReturn(List.of(mockKeyValue));

        when(mockKv.get(any(ByteSequence.class)))
            .thenReturn(CompletableFuture.completedFuture(mockResponse));

        // Execute
        io.clustercontroller.models.ClusterInformation.Version result = store.getClusterVersion(clusterId);

        // Verify
        assertThat(result).isNotNull();
        assertThat(result.getNumber()).isEqualTo("3.2.0");
        assertThat(result.getDistribution()).isEqualTo("opensearch");
        assertThat(result.getBuildType()).isEqualTo("tar");
        assertThat(result.getHash()).isEqualTo("abc123");
        
        verify(mockKv).get(any(ByteSequence.class));
    }

    @Test
    public void testGetClusterVersion_NotFound() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getClusterRegistryPath(clusterId))
            .thenReturn("/multi-cluster/clusters/test-cluster/metadata");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Mock GetResponse with empty result
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getKvs()).thenReturn(Collections.emptyList());

        when(mockKv.get(any(ByteSequence.class)))
            .thenReturn(CompletableFuture.completedFuture(mockResponse));

        // Execute
        io.clustercontroller.models.ClusterInformation.Version result = store.getClusterVersion(clusterId);

        // Verify
        assertThat(result).isNull();
        verify(mockKv).get(any(ByteSequence.class));
    }

    @Test
    public void testGetClusterVersion_NoVersionField() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getClusterRegistryPath(clusterId))
            .thenReturn("/multi-cluster/clusters/test-cluster/metadata");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Mock GetResponse with cluster metadata but no version field
        String metadataJson = "{"
            + "\"cluster_id\":\"test-cluster\","
            + "\"created_at\":\"2024-01-01\""
            + "}";
        
        KeyValue mockKeyValue = mock(KeyValue.class);
        when(mockKeyValue.getValue()).thenReturn(ByteSequence.from(metadataJson, UTF_8));
        
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getKvs()).thenReturn(List.of(mockKeyValue));

        when(mockKv.get(any(ByteSequence.class)))
            .thenReturn(CompletableFuture.completedFuture(mockResponse));

        // Execute
        io.clustercontroller.models.ClusterInformation.Version result = store.getClusterVersion(clusterId);

        // Verify
        assertThat(result).isNull();
        verify(mockKv).get(any(ByteSequence.class));
    }

    @Test
    public void testGetClusterVersion_EtcdException() throws Exception {
        EtcdMetadataStore store = newStore();
        String clusterId = "test-cluster";

        // Mock path resolver
        EtcdPathResolver mockPathResolver = mock(EtcdPathResolver.class);
        when(mockPathResolver.getClusterRegistryPath(clusterId))
            .thenReturn("/multi-cluster/clusters/test-cluster/metadata");
        setPrivateField(store, "pathResolver", mockPathResolver);

        // Mock etcd failure
        when(mockKv.get(any(ByteSequence.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd connection error")));

        // Verify exception is thrown
        assertThatThrownBy(() -> store.getClusterVersion(clusterId))
            .isInstanceOf(Exception.class)
            .hasMessageContaining("etcd connection error");
        
        verify(mockKv).get(any(ByteSequence.class));
    }

    // ------------------------- reflection util -------------------------

    private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }
}
