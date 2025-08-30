package io.clustercontroller;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Optional;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for TaskManager.
 */
class TaskManagerTest {
    
    @Mock
    private MetadataStore metadataStore;
    
    @Mock
    private TaskContext taskContext;
    
    private TaskManager taskManager;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        taskManager = new TaskManager(metadataStore, taskContext, 30L);
    }
    
    @Test
    void testTaskManagerCreation() {
        assertThat(taskManager).isNotNull();
    }
    
    @Test
    void testCreateTask() throws Exception {
        String taskName = "test-task";
        String input = "test-input";
        int priority = 1;
        
        when(metadataStore.createTask(any(TaskMetadata.class))).thenReturn(taskName);
        
        TaskMetadata result = taskManager.createTask(taskName, input, priority);
        
        assertThat(result.getName()).isEqualTo(taskName);
        assertThat(result.getInput()).isEqualTo(input);
        assertThat(result.getPriority()).isEqualTo(priority);
        assertThat(result.getStatus()).isEqualTo(TASK_STATUS_PENDING);
        
        verify(metadataStore).createTask(any(TaskMetadata.class));
    }
    
    @Test
    void testGetAllTasks() throws Exception {
        List<TaskMetadata> mockTasks = List.of(
            new TaskMetadata("task1", 1),
            new TaskMetadata("task2", 2)
        );
        
        when(metadataStore.getAllTasks()).thenReturn(mockTasks);
        
        List<TaskMetadata> result = taskManager.getAllTasks();
        
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getName()).isEqualTo("task1");
        assertThat(result.get(1).getName()).isEqualTo("task2");
        
        verify(metadataStore).getAllTasks();
    }
    
    @Test
    void testGetTask() throws Exception {
        String taskName = "test-task";
        TaskMetadata mockTask = new TaskMetadata(taskName, 1);
        
        when(metadataStore.getTask(taskName)).thenReturn(Optional.of(mockTask));
        
        Optional<TaskMetadata> result = taskManager.getTask(taskName);
        
        assertThat(result).isPresent();
        assertThat(result.get().getName()).isEqualTo(taskName);
        
        verify(metadataStore).getTask(taskName);
    }
    
    @Test
    void testDeleteTask() throws Exception {
        String taskName = "test-task";
        
        doNothing().when(metadataStore).deleteTask(taskName);
        
        taskManager.deleteTask(taskName);
        
        verify(metadataStore).deleteTask(taskName);
    }
}
