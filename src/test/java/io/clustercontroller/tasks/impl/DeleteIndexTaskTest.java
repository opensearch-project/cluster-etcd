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
 * Tests for DeleteIndexTask.
 */
class DeleteIndexTaskTest {
    
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
    void testDeleteIndexTaskExecution() {
        String taskName = "delete-index-task";
        String input = "index-config";
        DeleteIndexTask task = new DeleteIndexTask(taskName, 1, input, TASK_SCHEDULE_ONCE);
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
        verify(indexManager).deleteIndex(input);
    }
    
    @Test
    void testDeleteIndexTaskProperties() {
        String taskName = "delete-index-task";
        String input = "index-config";
        int priority = 3;
        String schedule = TASK_SCHEDULE_ONCE;
        
        DeleteIndexTask task = new DeleteIndexTask(taskName, priority, input, schedule);
        
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getPriority()).isEqualTo(priority);
        assertThat(task.getInput()).isEqualTo(input);
        assertThat(task.getSchedule()).isEqualTo(schedule);
    }
    
    @Test
    void testDeleteIndexTaskExecutionFailure() {
        String taskName = "delete-index-task";
        String input = "invalid-config";
        DeleteIndexTask task = new DeleteIndexTask(taskName, 1, input, TASK_SCHEDULE_ONCE);
        
        doThrow(new RuntimeException("Index deletion failed")).when(indexManager).deleteIndex(input);
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(indexManager).deleteIndex(input);
    }
}
