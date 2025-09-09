package io.clustercontroller.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for EtcdPathResolver.
 */
class EtcdPathResolverTest {
    
    private EtcdPathResolver pathResolver;
    private final String testClusterName = "test-cluster";
    
    @BeforeEach
    void setUp() {
        pathResolver = new EtcdPathResolver(testClusterName);
    }
    
    @Test
    void testGetClusterName() {
        assertThat(pathResolver.getClusterName()).isEqualTo(testClusterName);
    }
    
    @Test
    void testGetClusterRoot() {
        String result = pathResolver.getClusterRoot();
        assertThat(result).isEqualTo("/test-cluster");
    }
    
    // =================================================================
    // CONTROLLER TASKS PATHS TESTS
    // =================================================================
    
    @Test
    void testGetControllerTasksPrefix() {
        String result = pathResolver.getControllerTasksPrefix();
        assertThat(result).isEqualTo("/test-cluster/ctl-tasks");
    }
    
    @Test
    void testGetControllerTaskPath() {
        String result = pathResolver.getControllerTaskPath("create-index-task");
        assertThat(result).isEqualTo("/test-cluster/ctl-tasks/create-index-task");
    }
    
    // =================================================================
    // SEARCH UNIT PATHS TESTS
    // =================================================================
    
    @Test
    void testGetSearchUnitsPrefix() {
        String result = pathResolver.getSearchUnitsPrefix();
        assertThat(result).isEqualTo("/test-cluster/search-units");
    }
    
    @Test
    void testGetSearchUnitConfPath() {
        String result = pathResolver.getSearchUnitConfPath("unit-1");
        assertThat(result).isEqualTo("/test-cluster/search-units/unit-1/conf");
    }
    
    @Test
    void testGetSearchUnitGoalStatePath() {
        String result = pathResolver.getSearchUnitGoalStatePath("unit-1");
        assertThat(result).isEqualTo("/test-cluster/search-units/unit-1/goal-state");
    }
    
    @Test
    void testGetSearchUnitActualStatePath() {
        String result = pathResolver.getSearchUnitActualStatePath("unit-1");
        assertThat(result).isEqualTo("/test-cluster/search-units/unit-1/actual-state");
    }
    
    
    // =================================================================
    // INDEX PATHS TESTS
    // =================================================================
    
    @Test
    void testGetIndicesPrefix() {
        String result = pathResolver.getIndicesPrefix();
        assertThat(result).isEqualTo("/test-cluster/indices");
    }
    
    @Test
    void testGetIndexConfPath() {
        String result = pathResolver.getIndexConfPath("user-index");
        assertThat(result).isEqualTo("/test-cluster/indices/user-index/conf");
    }
    
    @Test
    void testGetIndexMappingsPath() {
        String result = pathResolver.getIndexMappingsPath("user-index");
        assertThat(result).isEqualTo("/test-cluster/indices/user-index/mappings");
    }
    
    @Test
    void testGetIndexSettingsPath() {
        String result = pathResolver.getIndexSettingsPath("user-index");
        assertThat(result).isEqualTo("/test-cluster/indices/user-index/settings");
    }
    
    // =================================================================
    // SHARD ALLOCATION PATHS TESTS
    // =================================================================
    
    @Test
    void testGetShardPlannedAllocationPath() {
        String result = pathResolver.getShardPlannedAllocationPath("user-index", "0");
        assertThat(result).isEqualTo("/test-cluster/indices/user-index/shard/0/planned-allocation");
    }
    
    @Test
    void testGetShardActualAllocationPath() {
        String result = pathResolver.getShardActualAllocationPath("user-index", "0");
        assertThat(result).isEqualTo("/test-cluster/indices/user-index/shard/0/actual-allocation");
    }
    
    // =================================================================
    // COORDINATOR PATHS TESTS
    // =================================================================
    
    @Test
    void testGetCoordinatorsPrefix() {
        String result = pathResolver.getCoordinatorsPrefix();
        assertThat(result).isEqualTo("/test-cluster/coordinators");
    }
    
    @Test
    void testGetCoordinatorGoalStatePath() {
        String result = pathResolver.getCoordinatorGoalStatePath();
        assertThat(result).isEqualTo("/test-cluster/coordinators/goal-state");
    }
    
    @Test
    void testGetCoordinatorActualStatePath() {
        String result = pathResolver.getCoordinatorActualStatePath("coord-1");
        assertThat(result).isEqualTo("/test-cluster/coordinators/coord-1/actual-state");
    }
    
    // =================================================================
    // LEADER ELECTION PATHS TESTS
    // =================================================================
    
    @Test
    void testGetLeaderElectionPath() {
        String result = pathResolver.getLeaderElectionPath();
        assertThat(result).isEqualTo("/test-cluster/leader-election");
    }
    
    // =================================================================
    // EDGE CASES TESTS
    // =================================================================
    
    @Test
    void testPathsWithSpecialCharacters() {
        String result = pathResolver.getControllerTaskPath("task-with-dashes_and_underscores");
        assertThat(result).isEqualTo("/test-cluster/ctl-tasks/task-with-dashes_and_underscores");
    }
    
    @Test
    void testPathsWithNumbers() {
        String result = pathResolver.getSearchUnitConfPath("unit-123");
        assertThat(result).isEqualTo("/test-cluster/search-units/unit-123/conf");
    }
    
    @Test
    void testClusterNameWithSpecialCharacters() {
        EtcdPathResolver specialResolver = new EtcdPathResolver("cluster-with-dashes");
        String result = specialResolver.getControllerTasksPrefix();
        assertThat(result).isEqualTo("/cluster-with-dashes/ctl-tasks");
    }
}
