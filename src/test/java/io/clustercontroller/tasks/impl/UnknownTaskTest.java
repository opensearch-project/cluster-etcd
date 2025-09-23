package io.clustercontroller.tasks.impl;

import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Tests for UnknownTask.
 */
class UnknownTaskTest {
    
    @Mock
    private TaskContext taskContext;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    void testUnknownTaskExecution() {
        String taskName = "unknown-task-type";
        String input = "some-input";
        UnknownTask task = new UnknownTask(taskName, 1, input, TASK_SCHEDULE_ONCE);
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        // No component interactions for unknown tasks
    }
    
    @Test
    void testUnknownTaskProperties() {
        String taskName = "unknown-task-type";
        String input = "some-input";
        int priority = 10;
        String schedule = TASK_SCHEDULE_ONCE;
        
        UnknownTask task = new UnknownTask(taskName, priority, input, schedule);
        
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getPriority()).isEqualTo(priority);
        assertThat(task.getInput()).isEqualTo(input);
        assertThat(task.getSchedule()).isEqualTo(schedule);
    }
}

