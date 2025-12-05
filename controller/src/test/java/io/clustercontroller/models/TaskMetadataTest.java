package io.clustercontroller.models;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.clustercontroller.config.Constants.*;

/**
 * Tests for TaskMetadata model.
 */
class TaskMetadataTest {
    
    @Test
    void testTaskMetadataCreation() {
        TaskMetadata task = new TaskMetadata();
        
        assertThat(task.getStatus()).isEqualTo(TASK_STATUS_PENDING);
        assertThat(task.getCreatedAt()).isNotNull();
        assertThat(task.getLastUpdated()).isNotNull();
    }
    
    @Test
    void testTaskMetadataWithParameters() {
        String taskName = "test-task";
        int priority = 5;
        
        TaskMetadata task = new TaskMetadata(taskName, priority);
        
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getPriority()).isEqualTo(priority);
        assertThat(task.getStatus()).isEqualTo(TASK_STATUS_PENDING);
    }
    
    @Test
    void testTaskMetadataSetters() {
        TaskMetadata task = new TaskMetadata();
        
        task.setName("test-task");
        task.setInput("test-input");
        task.setSchedule(TASK_SCHEDULE_REPEAT);
        
        assertThat(task.getName()).isEqualTo("test-task");
        assertThat(task.getInput()).isEqualTo("test-input");
        assertThat(task.getSchedule()).isEqualTo(TASK_SCHEDULE_REPEAT);
    }
    
    @Test
    void testTaskMetadataStatusUpdate() {
        TaskMetadata task = new TaskMetadata();
        
        task.setStatus(TASK_STATUS_RUNNING);
        
        assertThat(task.getStatus()).isEqualTo(TASK_STATUS_RUNNING);
        assertThat(task.getLastUpdated()).isNotNull();
    }
    
    @Test
    void testTaskMetadataToString() {
        TaskMetadata task = new TaskMetadata("test-task", 1);
        
        String taskString = task.toString();
        
        assertThat(taskString).contains("test-task");
        assertThat(taskString).contains("1");
        assertThat(taskString).contains(TASK_STATUS_PENDING);
    }
}
