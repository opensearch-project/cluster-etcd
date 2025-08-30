package io.clustercontroller.tasks;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.tasks.impl.*;
import org.junit.jupiter.api.Test;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for TaskFactory.
 */
class TaskFactoryTest {
    
    @Test
    void testCreateIndexTask() {
        TaskMetadata metadata = new TaskMetadata(TASK_ACTION_CREATE_INDEX, 1);
        metadata.setInput("index-config");
        
        Task task = TaskFactory.createTask(metadata);
        
        assertThat(task).isInstanceOf(CreateIndexTask.class);
        assertThat(task.getName()).isEqualTo(TASK_ACTION_CREATE_INDEX);
        assertThat(task.getPriority()).isEqualTo(1);
        assertThat(task.getInput()).isEqualTo("index-config");
    }
    
    @Test
    void testDeleteIndexTask() {
        TaskMetadata metadata = new TaskMetadata(TASK_ACTION_DELETE_INDEX, 2);
        
        Task task = TaskFactory.createTask(metadata);
        
        assertThat(task).isInstanceOf(DeleteIndexTask.class);
        assertThat(task.getName()).isEqualTo(TASK_ACTION_DELETE_INDEX);
    }
    
    @Test
    void testDiscoverSearchUnitTask() {
        TaskMetadata metadata = new TaskMetadata(TASK_ACTION_DISCOVER_SEARCH_UNIT, 3);
        
        Task task = TaskFactory.createTask(metadata);
        
        assertThat(task).isInstanceOf(DiscoverSearchUnitTask.class);
        assertThat(task.getName()).isEqualTo(TASK_ACTION_DISCOVER_SEARCH_UNIT);
    }
    
    @Test
    void testShardAllocatorTask() {
        TaskMetadata metadata = new TaskMetadata(TASK_ACTION_SHARD_ALLOCATOR, 4);
        
        Task task = TaskFactory.createTask(metadata);
        
        assertThat(task).isInstanceOf(ShardAllocatorTask.class);
        assertThat(task.getName()).isEqualTo(TASK_ACTION_SHARD_ALLOCATOR);
    }
    
    @Test
    void testActualAllocationUpdaterTask() {
        TaskMetadata metadata = new TaskMetadata(TASK_ACTION_ACTUAL_ALLOCATION_UPDATER, 5);
        
        Task task = TaskFactory.createTask(metadata);
        
        assertThat(task).isInstanceOf(ActualAllocationUpdaterTask.class);
        assertThat(task.getName()).isEqualTo(TASK_ACTION_ACTUAL_ALLOCATION_UPDATER);
    }
    
    @Test
    void testPlanShardAllocationTask() {
        TaskMetadata metadata = new TaskMetadata(TASK_ACTION_PLAN_SHARD_ALLOCATION, 6);
        
        Task task = TaskFactory.createTask(metadata);
        
        assertThat(task).isInstanceOf(PlanShardAllocationTask.class);
        assertThat(task.getName()).isEqualTo(TASK_ACTION_PLAN_SHARD_ALLOCATION);
    }
    
    @Test
    void testUnknownTask() {
        TaskMetadata metadata = new TaskMetadata("unknown-task-type", 7);
        
        Task task = TaskFactory.createTask(metadata);
        
        assertThat(task).isInstanceOf(UnknownTask.class);
        assertThat(task.getName()).isEqualTo("unknown-task-type");
    }
}
