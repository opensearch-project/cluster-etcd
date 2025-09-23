package io.clustercontroller.tasks.impl;

import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for ShardAllocatorTask.
 */
class ShardAllocatorTaskTest {
    
    @Mock
    private TaskContext taskContext;
    
    @Mock
    private ShardAllocator shardAllocator;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(taskContext.getShardAllocator()).thenReturn(shardAllocator);
    }
    
    @Test
    void testShardAllocatorTaskExecution() {
        String taskName = "shard-allocator-task";
        String input = "";
        ShardAllocatorTask task = new ShardAllocatorTask(taskName, 1, input, TASK_SCHEDULE_REPEAT);
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
        verify(shardAllocator).allocateShards();
    }
    
    @Test
    void testShardAllocatorTaskProperties() {
        String taskName = "shard-allocator-task";
        String input = "";
        int priority = 4;
        String schedule = TASK_SCHEDULE_REPEAT;
        
        ShardAllocatorTask task = new ShardAllocatorTask(taskName, priority, input, schedule);
        
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getPriority()).isEqualTo(priority);
        assertThat(task.getInput()).isEqualTo(input);
        assertThat(task.getSchedule()).isEqualTo(schedule);
    }
    
    @Test
    void testShardAllocatorTaskExecutionFailure() {
        String taskName = "shard-allocator-task";
        String input = "";
        ShardAllocatorTask task = new ShardAllocatorTask(taskName, 1, input, TASK_SCHEDULE_REPEAT);
        
        doThrow(new RuntimeException("Shard allocation failed")).when(shardAllocator).allocateShards();
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(shardAllocator).allocateShards();
    }
}

