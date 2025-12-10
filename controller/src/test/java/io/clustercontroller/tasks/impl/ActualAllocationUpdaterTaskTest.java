package io.clustercontroller.tasks.impl;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for ActualAllocationUpdaterTask.
 */
class ActualAllocationUpdaterTaskTest {
    
    @Mock
    private TaskContext taskContext;
    
    @Mock
    private ActualAllocationUpdater actualAllocationUpdater;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(taskContext.getActualAllocationUpdater()).thenReturn(actualAllocationUpdater);
    }
    
    @Test
    void testActualAllocationUpdaterTaskExecution() throws Exception {
        String taskName = "actual-allocation-updater-task";
        String input = "";
        ActualAllocationUpdaterTask task = new ActualAllocationUpdaterTask(taskName, 1, input, TASK_SCHEDULE_REPEAT);
        
        String result = task.execute(taskContext, "test-cluster");
        
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
        verify(actualAllocationUpdater).updateActualAllocations("test-cluster");
    }
    
    @Test
    void testActualAllocationUpdaterTaskProperties() {
        String taskName = "actual-allocation-updater-task";
        String input = "";
        int priority = 6;
        String schedule = TASK_SCHEDULE_REPEAT;
        
        ActualAllocationUpdaterTask task = new ActualAllocationUpdaterTask(taskName, priority, input, schedule);
        
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getPriority()).isEqualTo(priority);
        assertThat(task.getInput()).isEqualTo(input);
        assertThat(task.getSchedule()).isEqualTo(schedule);
    }
    
    @Test
    void testActualAllocationUpdaterTaskExecutionFailure() throws Exception {
        String taskName = "actual-allocation-updater-task";
        String input = "";
        ActualAllocationUpdaterTask task = new ActualAllocationUpdaterTask(taskName, 1, input, TASK_SCHEDULE_REPEAT);
        
        doThrow(new RuntimeException("Allocation update failed")).when(actualAllocationUpdater).updateActualAllocations("test-cluster");
        
        String result = task.execute(taskContext, "test-cluster");
        
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(actualAllocationUpdater).updateActualAllocations("test-cluster");
    }
}
