package io.clustercontroller.models;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.clustercontroller.config.Constants.*;

/**
 * Tests for Task model.
 */
class TaskTest {
    
    @Test
    void testTaskCreation() {
        Task task = new Task();
        
        assertThat(task.getStatus()).isEqualTo(TASK_STATUS_PENDING);
        assertThat(task.getCreatedAt()).isNotNull();
        assertThat(task.getLastUpdated()).isNotNull();
    }
    
    @Test
    void testTaskWithParameters() {
        String taskName = "test-task";
        int priority = 5;
        
        Task task = new Task(taskName, priority);
        
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getPriority()).isEqualTo(priority);
        assertThat(task.getStatus()).isEqualTo(TASK_STATUS_PENDING);
    }
    
    @Test
    void testTaskSetters() {
        Task task = new Task();
        
        task.setName("test-task");
        task.setInput("test-input");
        task.setSchedule(TASK_SCHEDULE_REPEAT);
        
        assertThat(task.getName()).isEqualTo("test-task");
        assertThat(task.getInput()).isEqualTo("test-input");
        assertThat(task.getSchedule()).isEqualTo(TASK_SCHEDULE_REPEAT);
    }
    
    @Test
    void testTaskStatusUpdate() {
        Task task = new Task();
        
        task.setStatus(TASK_STATUS_RUNNING);
        
        assertThat(task.getStatus()).isEqualTo(TASK_STATUS_RUNNING);
        assertThat(task.getLastUpdated()).isNotNull();
    }
    
    @Test
    void testTaskToString() {
        Task task = new Task("test-task", 1);
        
        String taskString = task.toString();
        
        assertThat(taskString).contains("test-task");
        assertThat(taskString).contains("1");
        assertThat(taskString).contains(TASK_STATUS_PENDING);
    }
}
