package io.clustercontroller.health;

import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterHealthManagerTest {

    @Mock
    private Discovery discovery;

    @Mock
    private MetadataStore metadataStore;

    @InjectMocks
    private ClusterHealthManager clusterHealthManager;
    
    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetClusterHealth_NotImplemented() {
        assertThatThrownBy(() -> clusterHealthManager.getClusterHealth(testClusterId, "cluster"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster health not yet implemented");
    }

    @Test
    void testGetClusterHealth_WithIndicesLevel_NotImplemented() {
        assertThatThrownBy(() -> clusterHealthManager.getClusterHealth(testClusterId, "indices"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster health not yet implemented");
    }

    @Test
    void testGetClusterHealth_WithShardsLevel_NotImplemented() {
        assertThatThrownBy(() -> clusterHealthManager.getClusterHealth(testClusterId, "shards"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster health not yet implemented");
    }

    @Test
    void testGetIndexHealth_NotImplemented() {
        String indexName = "test-index";
        assertThatThrownBy(() -> clusterHealthManager.getIndexHealth(testClusterId, indexName, "indices"))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Index health not yet implemented");
    }

    @Test
    void testGetClusterStats_NotImplemented() {
        assertThatThrownBy(() -> clusterHealthManager.getClusterStats(testClusterId))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cluster stats not yet implemented");
    }
}