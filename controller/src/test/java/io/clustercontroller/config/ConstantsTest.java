package io.clustercontroller.config;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.clustercontroller.config.Constants.*;

/**
 * Tests for Constants.
 */
class ConstantsTest {
    
    @Test
    void testDefaultConfigurationConstants() {
        assertThat(DEFAULT_ETCD_ENDPOINT).isEqualTo("http://localhost:2379");
        assertThat(DEFAULT_TASK_INTERVAL_SECONDS).isEqualTo(30L);
    }
    
    @Test
    void testTaskStatusConstants() {
        assertThat(TASK_STATUS_PENDING).isEqualTo("PENDING");
        assertThat(TASK_STATUS_RUNNING).isEqualTo("RUNNING");
        assertThat(TASK_STATUS_COMPLETED).isEqualTo("COMPLETED");
        assertThat(TASK_STATUS_FAILED).isEqualTo("FAILED");
    }
    
    @Test
    void testTaskScheduleConstants() {
        assertThat(TASK_SCHEDULE_ONCE).isEqualTo("once");
        assertThat(TASK_SCHEDULE_REPEAT).isEqualTo("repeat");
    }
    
    @Test
    void testTaskActionConstants() {
        assertThat(TASK_ACTION_CREATE_INDEX).isEqualTo("create_index");
        assertThat(TASK_ACTION_DELETE_INDEX).isEqualTo("delete_index");
        assertThat(TASK_ACTION_DISCOVERY).isEqualTo("discovery");
        assertThat(TASK_ACTION_PLAN_SHARD_ALLOCATION).isEqualTo("plan_shard_allocation");
        assertThat(TASK_ACTION_SHARD_ALLOCATOR).isEqualTo("shard_allocator");
        assertThat(TASK_ACTION_ACTUAL_ALLOCATION_UPDATER).isEqualTo("actual_allocation_updater");
    }
    
    @Test
    void testConstantsClassCannotBeInstantiated() throws Exception {
        // Verify Constants is a proper utility class with private constructor
        var constructor = Constants.class.getDeclaredConstructor();
        assertThat(constructor.canAccess(null)).isFalse();
    }
}
