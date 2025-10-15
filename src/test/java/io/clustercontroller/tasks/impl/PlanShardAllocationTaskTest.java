package io.clustercontroller.tasks.impl;

import io.clustercontroller.allocation.AllocationStrategy;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PlanShardAllocationTaskTest {

    @Mock(lenient = true)
    private TaskContext taskContext;
    
    @Mock(lenient = true)
    private ShardAllocator shardAllocator;
    
    @BeforeEach
    void setUp() {
        // Mock the TaskContext to return a cluster name and shard allocator (lenient for tests that don't use it)
        when(taskContext.getClusterName()).thenReturn("test-cluster");
        when(taskContext.getShardAllocator()).thenReturn(shardAllocator);
        
        // Mock the shardAllocator to do nothing (successful execution)
        doNothing().when(shardAllocator).planShardAllocation(anyString(), any(AllocationStrategy.class));
    }
    
    @Test
    void testPlanShardAllocationTaskExecution_ReturnsCompleted() {
        // Given
        String taskName = "plan-shard-allocation-task";
        String input = "";
        PlanShardAllocationTask task = new PlanShardAllocationTask(taskName, 1, input, TASK_SCHEDULE_REPEAT);
        
        // When
        String result = task.execute(taskContext);
        
        // Then
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
    }
    
    @Test
    void testGetName() {
        // Given
        String taskName = "test-plan-shard-allocation";
        PlanShardAllocationTask task = new PlanShardAllocationTask(taskName, 1, "", TASK_SCHEDULE_ONCE);
        
        // When
        String name = task.getName();
        
        // Then
        assertThat(name).isEqualTo(taskName);
    }
    
    @Test
    void testGetPriority() {
        // Given
        int priority = 5;
        PlanShardAllocationTask task = new PlanShardAllocationTask("test", priority, "", TASK_SCHEDULE_ONCE);
        
        // When
        int actualPriority = task.getPriority();
        
        // Then
        assertThat(actualPriority).isEqualTo(priority);
    }
    
    @Test
    void testGetInput() {
        // Given
        String input = "test-input";
        PlanShardAllocationTask task = new PlanShardAllocationTask("test", 1, input, TASK_SCHEDULE_ONCE);
        
        // When
        String actualInput = task.getInput();
        
        // Then
        assertThat(actualInput).isEqualTo(input);
    }
    
    @Test
    void testGetSchedule() {
        // Given
        String schedule = TASK_SCHEDULE_REPEAT;
        PlanShardAllocationTask task = new PlanShardAllocationTask("test", 1, "", schedule);
        
        // When
        String actualSchedule = task.getSchedule();
        
        // Then
        assertThat(actualSchedule).isEqualTo(schedule);
    }
}