package io.clustercontroller.tasks.impl;

import io.clustercontroller.indices.IndexManager;
import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for PlanShardAllocationTask.
 */
class PlanShardAllocationTaskTest {
    
    @Mock
    private TaskContext taskContext;
    
    @Mock
    private IndexManager indexManager;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(taskContext.getIndexManager()).thenReturn(indexManager);
    }
    
    @Test
    void testPlanShardAllocationTaskExecution() {
        String taskName = "plan-shard-allocation-task";
        String input = "";
        PlanShardAllocationTask task = new PlanShardAllocationTask(taskName, 1, input, TASK_SCHEDULE_ONCE);
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
        verify(indexManager).planShardAllocation();
    }
    
    @Test
    void testPlanShardAllocationTaskProperties() {
        String taskName = "plan-shard-allocation-task";
        String input = "";
        int priority = 7;
        String schedule = TASK_SCHEDULE_ONCE;
        
        PlanShardAllocationTask task = new PlanShardAllocationTask(taskName, priority, input, schedule);
        
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getPriority()).isEqualTo(priority);
        assertThat(task.getInput()).isEqualTo(input);
        assertThat(task.getSchedule()).isEqualTo(schedule);
    }
    
    @Test
    void testPlanShardAllocationTaskExecutionFailure() {
        String taskName = "plan-shard-allocation-task";
        String input = "";
        PlanShardAllocationTask task = new PlanShardAllocationTask(taskName, 1, input, TASK_SCHEDULE_ONCE);
        
        doThrow(new RuntimeException("Planning failed")).when(indexManager).planShardAllocation();
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(indexManager).planShardAllocation();
    }
}
