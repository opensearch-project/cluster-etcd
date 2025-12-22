package io.clustercontroller.metrics;

import io.clustercontroller.enums.NodeRole;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsUtilsTest {

    @Test
    void testBuildMetricsTags() {
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";
        Map<String, String> tags = MetricsUtils.buildMetricsTags(clusterId, indexName, shardId);
        assertThat(tags)
            .hasSize(3)
            .containsEntry("clusterId", clusterId)
            .containsEntry("indexName", indexName)
            .containsEntry("shardId", shardId);
    }

    @Test
    void testBuildMetricsTagsByRole() {
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";
        NodeRole role = NodeRole.REPLICA;
        Map<String, String> tags = MetricsUtils.buildMetricsTagsByRole(clusterId, indexName, shardId, role);
        assertThat(tags)
            .hasSize(4)
            .containsEntry("clusterId", clusterId)
            .containsEntry("indexName", indexName)
            .containsEntry("shardId", shardId)
            .containsEntry("role", "SEARCH_REPLICA"); // Uses getValue(), not name()
    }
}

