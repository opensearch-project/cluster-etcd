package io.clustercontroller.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EtcdPathResolverTest {

    private EtcdPathResolver pathResolver;
    private final String testClusterName = "test-cluster";

    @BeforeEach
    void setUp() {
        pathResolver = EtcdPathResolver.getInstance();
    }

    @Test
    void testSingletonPattern() {
        EtcdPathResolver instance1 = EtcdPathResolver.getInstance();
        EtcdPathResolver instance2 = EtcdPathResolver.getInstance();
        assertThat(instance1).isSameAs(instance2);
    }

    @Test
    void testGetClusterRoot() {
        String path = pathResolver.getClusterRoot(testClusterName);
        assertThat(path).isEqualTo("/test-cluster");
    }

    @Test
    void testGetControllerTasksPrefix() {
        String path = pathResolver.getControllerTasksPrefix(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/ctl-tasks");
    }

    @Test
    void testGetControllerTaskPath() {
        String path = pathResolver.getControllerTaskPath(testClusterName, "task1");
        assertThat(path).isEqualTo("/test-cluster/ctl-tasks/task1");
    }

    @Test
    void testGetSearchUnitsPrefix() {
        String path = pathResolver.getSearchUnitsPrefix(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/search-units");
    }

    @Test
    void testGetSearchUnitConfPath() {
        String path = pathResolver.getSearchUnitConfPath(testClusterName, "unit1");
        assertThat(path).isEqualTo("/test-cluster/search-units/unit1/conf");
    }

    @Test
    void testGetSearchUnitGoalStatePath() {
        String path = pathResolver.getSearchUnitGoalStatePath(testClusterName, "unit1");
        assertThat(path).isEqualTo("/test-cluster/search-units/unit1/goal-state");
    }

    @Test
    void testGetSearchUnitActualStatePath() {
        String path = pathResolver.getSearchUnitActualStatePath(testClusterName, "unit1");
        assertThat(path).isEqualTo("/test-cluster/search-units/unit1/actual-state");
    }

    @Test
    void testGetIndicesPrefix() {
        String path = pathResolver.getIndicesPrefix(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/indices");
    }

    @Test
    void testGetIndexConfPath() {
        String path = pathResolver.getIndexConfPath(testClusterName, "index1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/conf");
    }

    @Test
    void testGetIndexMappingsPath() {
        String path = pathResolver.getIndexMappingsPath(testClusterName, "index1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/mappings");
    }

    @Test
    void testGetIndexSettingsPath() {
        String path = pathResolver.getIndexSettingsPath(testClusterName, "index1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/settings");
    }

    @Test
    void testGetShardPlannedAllocationPath() {
        String path = pathResolver.getShardPlannedAllocationPath(testClusterName, "index1", "shard1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/shard1/planned-allocation");
    }

    @Test
    void testGetShardActualAllocationPath() {
        String path = pathResolver.getShardActualAllocationPath(testClusterName, "index1", "shard1");
        assertThat(path).isEqualTo("/test-cluster/indices/index1/shard1/actual-allocation");
    }

    @Test
    void testGetCoordinatorsPrefix() {
        String path = pathResolver.getCoordinatorsPrefix(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/coordinators");
    }

    @Test
    void testGetCoordinatorGoalStatePath() {
        String path = pathResolver.getCoordinatorGoalStatePath(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/coordinators/goal-state");
    }

    @Test
    void testGetCoordinatorActualStatePath() {
        String path = pathResolver.getCoordinatorActualStatePath(testClusterName, "coord1");
        assertThat(path).isEqualTo("/test-cluster/coordinators/coord1/actual-state");
    }

    @Test
    void testGetLeaderElectionPath() {
        String path = pathResolver.getLeaderElectionPath(testClusterName);
        assertThat(path).isEqualTo("/test-cluster/leader-election");
    }

    @Test
    void testMultipleClusterNames() {
        String cluster1Path = pathResolver.getIndexConfPath("cluster1", "index1");
        String cluster2Path = pathResolver.getIndexConfPath("cluster2", "index1");
        
        assertThat(cluster1Path).isEqualTo("/cluster1/indices/index1/conf");
        assertThat(cluster2Path).isEqualTo("/cluster2/indices/index1/conf");
        assertThat(cluster1Path).isNotEqualTo(cluster2Path);
    }
}