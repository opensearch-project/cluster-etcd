package io.clustercontroller;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class TaskManagerTest {

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private TaskContext taskContext;

    private TaskManager taskManager;
    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        taskManager = new TaskManager(metadataStore, taskContext, testClusterId, 30L);
    }

    @Test
    void testCreateTask_Success() throws Exception {
        // Given
        String taskName = "test-task";
        String input = "test-input";
        int priority = 1;

        when(metadataStore.createTask(anyString(), any(TaskMetadata.class))).thenReturn("task-id");

        // When
        TaskMetadata task = taskManager.createTask(taskName, input, priority);

        // Then
        assertThat(task).isNotNull();
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getInput()).isEqualTo(input);
        assertThat(task.getPriority()).isEqualTo(priority);

        verify(metadataStore).createTask(eq(testClusterId), any(TaskMetadata.class));
    }

    @Test
    void testGetTasks_Success() throws Exception {
        // Given
        TaskMetadata task = new TaskMetadata();
        task.setName("test-task");
        task.setCreatedAt(OffsetDateTime.now());

        when(metadataStore.getAllTasks(anyString())).thenReturn(Collections.singletonList(task));

        // When
        var tasks = taskManager.getAllTasks();

        // Then
        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(0).getName()).isEqualTo("test-task");

        verify(metadataStore).getAllTasks(testClusterId);
    }

    @Test
    void testGetTaskByName_Found() throws Exception {
        // Given
        String taskName = "test-task";
        TaskMetadata task = new TaskMetadata();
        task.setName(taskName);

        when(metadataStore.getTask(anyString(), anyString())).thenReturn(Optional.of(task));

        // When
        Optional<TaskMetadata> result = taskManager.getTask(taskName);

        // Then
        assertThat(result).isPresent();
        assertThat(result.get().getName()).isEqualTo(taskName);

        verify(metadataStore).getTask(testClusterId, taskName);
    }

    @Test
    void testGetTaskByName_NotFound() throws Exception {
        // Given
        String taskName = "missing-task";

        when(metadataStore.getTask(anyString(), anyString())).thenReturn(Optional.empty());

        // When
        Optional<TaskMetadata> result = taskManager.getTask(taskName);

        // Then
        assertThat(result).isEmpty();

        verify(metadataStore).getTask(testClusterId, taskName);
    }
}