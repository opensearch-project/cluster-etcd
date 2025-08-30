package io.clustercontroller.store;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for EtcdMetadataStore.
 */
class EtcdMetadataStoreTest {
    
    private EtcdMetadataStore metadataStore;
    
    @BeforeEach
    void setUp() throws Exception {
        String[] etcdEndpoints = {"localhost:2379"};
        metadataStore = EtcdMetadataStore.getInstance("test-cluster", etcdEndpoints);
    }
    
    @Test
    void testSingletonInstance() throws Exception {
        String[] etcdEndpoints = {"localhost:2379"};
        EtcdMetadataStore instance1 = EtcdMetadataStore.getInstance("test-cluster", etcdEndpoints);
        EtcdMetadataStore instance2 = EtcdMetadataStore.getInstance();
        
        assertThat(instance1).isSameAs(instance2);
    }
    
    @Test
    void testGetClusterName() {
        assertThat(metadataStore.getClusterName()).isEqualTo("test-cluster");
    }
    
    @Test
    void testGetAllTasksReturnsEmpty() throws Exception {
        List<TaskMetadata> tasks = metadataStore.getAllTasks();
        
        assertThat(tasks).isNotNull();
        assertThat(tasks).isEmpty();
    }
    
    @Test
    void testGetTaskReturnsEmpty() throws Exception {
        Optional<TaskMetadata> task = metadataStore.getTask("non-existent-task");
        
        assertThat(task).isEmpty();
    }
    
    @Test
    void testCreateTaskReturnsName() throws Exception {
        TaskMetadata task = new TaskMetadata("test-task", 1);
        
        String result = metadataStore.createTask(task);
        
        assertThat(result).isEqualTo("test-task");
    }
    
    @Test
    void testGetAllSearchUnitsReturnsEmpty() throws Exception {
        List<SearchUnit> units = metadataStore.getAllSearchUnits();
        
        assertThat(units).isNotNull();
        assertThat(units).isEmpty();
    }
    
    @Test
    void testGetSearchUnitReturnsEmpty() throws Exception {
        Optional<SearchUnit> unit = metadataStore.getSearchUnit("non-existent-unit");
        
        assertThat(unit).isEmpty();
    }
    
    @Test
    void testGetAllIndexConfigsReturnsEmpty() throws Exception {
        List<String> configs = metadataStore.getAllIndexConfigs();
        
        assertThat(configs).isNotNull();
        assertThat(configs).isEmpty();
    }
    
    @Test
    void testGetIndexConfigReturnsEmpty() throws Exception {
        Optional<String> config = metadataStore.getIndexConfig("non-existent-index");
        
        assertThat(config).isEmpty();
    }
    
    @Test
    void testCreateIndexConfigReturnsName() throws Exception {
        String indexName = "test-index";
        String indexConfig = "{}";
        
        String result = metadataStore.createIndexConfig(indexName, indexConfig);
        
        assertThat(result).isEqualTo(indexName);
    }
}
